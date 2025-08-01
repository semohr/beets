from __future__ import annotations

import os
import string
import sys
import time
import unicodedata
from functools import cached_property
from pathlib import Path
from typing import TYPE_CHECKING

from mediafile import MediaFile, UnreadableFileError

import beets
from beets import dbcore, logging, plugins, util
from beets.dbcore import types
from beets.util import (
    MoveOperation,
    bytestring_path,
    cached_classproperty,
    normpath,
    samefile,
    syspath,
)
from beets.util.functemplate import Template, template

from .exceptions import FileOperationError, ReadError, WriteError
from .queries import PF_KEY_DEFAULT, parse_query_string

if TYPE_CHECKING:
    from ..dbcore.query import FieldQuery, FieldQueryType
    from .library import Library  # noqa: F401

log = logging.getLogger("beets")


class LibModel(dbcore.Model["Library"]):
    """Shared concrete functionality for Items and Albums."""

    # Config key that specifies how an instance should be formatted.
    _format_config_key: str
    path: bytes

    @cached_classproperty
    def _types(cls) -> dict[str, types.Type]:
        """Return the types of the fields in this model."""
        return {
            **plugins.types(cls),  # type: ignore[arg-type]
            "data_source": types.STRING,
        }

    @cached_classproperty
    def _queries(cls) -> dict[str, FieldQueryType]:
        return plugins.named_queries(cls)  # type: ignore[arg-type]

    @cached_classproperty
    def writable_media_fields(cls) -> set[str]:
        return set(MediaFile.fields()) & cls._fields.keys()

    @property
    def filepath(self) -> Path:
        """The path to the entity as pathlib.Path."""
        return Path(os.fsdecode(self.path))

    def _template_funcs(self):
        funcs = DefaultTemplateFunctions(self, self._db).functions()
        funcs.update(plugins.template_funcs())
        return funcs

    def store(self, fields=None):
        super().store(fields)
        plugins.send("database_change", lib=self._db, model=self)

    def remove(self):
        super().remove()
        plugins.send("database_change", lib=self._db, model=self)

    def add(self, lib=None):
        # super().add() calls self.store(), which sends `database_change`,
        # so don't do it here
        super().add(lib)

    def __format__(self, spec):
        if not spec:
            spec = beets.config[self._format_config_key].as_str()
        assert isinstance(spec, str)
        return self.evaluate_template(spec)

    def __str__(self):
        return format(self)

    def __bytes__(self):
        return self.__str__().encode("utf-8")

    # Convenient queries.

    @classmethod
    def field_query(
        cls, field: str, pattern: str, query_cls: FieldQueryType
    ) -> FieldQuery:
        """Get a `FieldQuery` for the given field on this model."""
        fast = field in cls.all_db_fields
        if field in cls.shared_db_fields:
            # This field exists in both tables, so SQLite will encounter
            # an OperationalError if we try to use it in a query.
            # Using an explicit table name resolves this.
            field = f"{cls._table}.{field}"

        return query_cls(field, pattern, fast)

    @classmethod
    def any_field_query(cls, *args, **kwargs) -> dbcore.OrQuery:
        return dbcore.OrQuery(
            [cls.field_query(f, *args, **kwargs) for f in cls._search_fields]
        )

    @classmethod
    def any_writable_media_field_query(cls, *args, **kwargs) -> dbcore.OrQuery:
        fields = cls.writable_media_fields
        return dbcore.OrQuery(
            [cls.field_query(f, *args, **kwargs) for f in fields]
        )

    def duplicates_query(self, fields: list[str]) -> dbcore.AndQuery:
        """Return a query for entities with same values in the given fields."""
        return dbcore.AndQuery(
            [
                self.field_query(f, self.get(f), dbcore.MatchQuery)
                for f in fields
            ]
        )


class FormattedItemMapping(dbcore.db.FormattedMapping):
    """Add lookup for album-level fields.

    Album-level fields take precedence if `for_path` is true.
    """

    ALL_KEYS = "*"

    def __init__(self, item, included_keys=ALL_KEYS, for_path=False):
        # We treat album and item keys specially here,
        # so exclude transitive album keys from the model's keys.
        super().__init__(item, included_keys=[], for_path=for_path)
        self.included_keys = included_keys
        if included_keys == self.ALL_KEYS:
            # Performance note: this triggers a database query.
            self.model_keys = item.keys(computed=True, with_album=False)
        else:
            self.model_keys = included_keys
        self.item = item

    @cached_property
    def all_keys(self):
        return set(self.model_keys).union(self.album_keys)

    @cached_property
    def album_keys(self):
        album_keys = []
        if self.album:
            if self.included_keys == self.ALL_KEYS:
                # Performance note: this triggers a database query.
                for key in self.album.keys(computed=True):
                    if (
                        key in Album.item_keys
                        or key not in self.item._fields.keys()
                    ):
                        album_keys.append(key)
            else:
                album_keys = self.included_keys
        return album_keys

    @property
    def album(self):
        return self.item._cached_album

    def _get(self, key):
        """Get the value for a key, either from the album or the item.

        Raise a KeyError for invalid keys.
        """
        if self.for_path and key in self.album_keys:
            return self._get_formatted(self.album, key)
        elif key in self.model_keys:
            return self._get_formatted(self.model, key)
        elif key in self.album_keys:
            return self._get_formatted(self.album, key)
        else:
            raise KeyError(key)

    def __getitem__(self, key):
        """Get the value for a key.

        `artist` and `albumartist` are fallback values for each other
        when not set.
        """
        value = self._get(key)

        # `artist` and `albumartist` fields fall back to one another.
        # This is helpful in path formats when the album artist is unset
        # on as-is imports.
        try:
            if key == "artist" and not value:
                return self._get("albumartist")
            elif key == "albumartist" and not value:
                return self._get("artist")
        except KeyError:
            pass

        return value

    def __iter__(self):
        return iter(self.all_keys)

    def __len__(self):
        return len(self.all_keys)


class Album(LibModel):
    """Provide access to information about albums stored in a
    library.

    Reflects the library's "albums" table, including album art.
    """

    artpath: bytes

    _table = "albums"
    _flex_table = "album_attributes"
    _always_dirty = True
    _fields = {
        "id": types.PRIMARY_ID,
        "artpath": types.NullPathType(),
        "added": types.DATE,
        "albumartist": types.STRING,
        "albumartist_sort": types.STRING,
        "albumartist_credit": types.STRING,
        "albumartists": types.MULTI_VALUE_DSV,
        "albumartists_sort": types.MULTI_VALUE_DSV,
        "albumartists_credit": types.MULTI_VALUE_DSV,
        "album": types.STRING,
        "genre": types.STRING,
        "style": types.STRING,
        "discogs_albumid": types.INTEGER,
        "discogs_artistid": types.INTEGER,
        "discogs_labelid": types.INTEGER,
        "year": types.PaddedInt(4),
        "month": types.PaddedInt(2),
        "day": types.PaddedInt(2),
        "disctotal": types.PaddedInt(2),
        "comp": types.BOOLEAN,
        "mb_albumid": types.STRING,
        "mb_albumartistid": types.STRING,
        "mb_albumartistids": types.MULTI_VALUE_DSV,
        "albumtype": types.STRING,
        "albumtypes": types.SEMICOLON_SPACE_DSV,
        "label": types.STRING,
        "barcode": types.STRING,
        "mb_releasegroupid": types.STRING,
        "release_group_title": types.STRING,
        "asin": types.STRING,
        "catalognum": types.STRING,
        "script": types.STRING,
        "language": types.STRING,
        "country": types.STRING,
        "albumstatus": types.STRING,
        "albumdisambig": types.STRING,
        "releasegroupdisambig": types.STRING,
        "rg_album_gain": types.NULL_FLOAT,
        "rg_album_peak": types.NULL_FLOAT,
        "r128_album_gain": types.NULL_FLOAT,
        "original_year": types.PaddedInt(4),
        "original_month": types.PaddedInt(2),
        "original_day": types.PaddedInt(2),
    }

    _search_fields = ("album", "albumartist", "genre")

    @cached_classproperty
    def _types(cls) -> dict[str, types.Type]:
        return {**super()._types, "path": types.PathType()}

    _sorts = {
        "albumartist": dbcore.query.SmartArtistSort,
        "artist": dbcore.query.SmartArtistSort,
    }

    # List of keys that are set on an album's items.
    item_keys = [
        "added",
        "albumartist",
        "albumartists",
        "albumartist_sort",
        "albumartists_sort",
        "albumartist_credit",
        "albumartists_credit",
        "album",
        "genre",
        "style",
        "discogs_albumid",
        "discogs_artistid",
        "discogs_labelid",
        "year",
        "month",
        "day",
        "disctotal",
        "comp",
        "mb_albumid",
        "mb_albumartistid",
        "mb_albumartistids",
        "albumtype",
        "albumtypes",
        "label",
        "barcode",
        "mb_releasegroupid",
        "asin",
        "catalognum",
        "script",
        "language",
        "country",
        "albumstatus",
        "albumdisambig",
        "releasegroupdisambig",
        "release_group_title",
        "rg_album_gain",
        "rg_album_peak",
        "r128_album_gain",
        "original_year",
        "original_month",
        "original_day",
    ]

    _format_config_key = "format_album"

    @cached_classproperty
    def _relation(cls) -> type[Item]:
        return Item

    @cached_classproperty
    def relation_join(cls) -> str:
        """Return FROM clause which joins on related album items.

        Use LEFT join to select all albums, including those that do not have
        any items.
        """
        return (
            f"LEFT JOIN {cls._relation._table} "
            f"ON {cls._table}.id = {cls._relation._table}.album_id"
        )

    @property
    def art_filepath(self) -> Path | None:
        """The path to album's cover picture as pathlib.Path."""
        return Path(os.fsdecode(self.artpath)) if self.artpath else None

    @classmethod
    def _getters(cls):
        # In addition to plugin-provided computed fields, also expose
        # the album's directory as `path`.
        getters = plugins.album_field_getters()
        getters["path"] = Album.item_dir
        getters["albumtotal"] = Album._albumtotal
        return getters

    def items(self):
        """Return an iterable over the items associated with this
        album.

        This method conflicts with :meth:`LibModel.items`, which is
        inherited from :meth:`beets.dbcore.Model.items`.
        Since :meth:`Album.items` predates these methods, and is
        likely to be used by plugins, we keep this interface as-is.
        """
        return self._db.items(dbcore.MatchQuery("album_id", self.id))

    def remove(self, delete=False, with_items=True):
        """Remove this album and all its associated items from the
        library.

        If delete, then the items' files are also deleted from disk,
        along with any album art. The directories containing the album are
        also removed (recursively) if empty.

        Set with_items to False to avoid removing the album's items.
        """
        super().remove()

        # Send a 'album_removed' signal to plugins
        plugins.send("album_removed", album=self)

        # Delete art file.
        if delete:
            artpath = self.artpath
            if artpath:
                util.remove(artpath)

        # Remove (and possibly delete) the constituent items.
        if with_items:
            for item in self.items():
                item.remove(delete, False)

    def move_art(self, operation=MoveOperation.MOVE):
        """Move, copy, link or hardlink (depending on `operation`) any
        existing album art so that it remains in the same directory as
        the items.

        `operation` should be an instance of `util.MoveOperation`.
        """
        old_art = self.artpath
        if not old_art:
            return

        if not os.path.exists(syspath(old_art)):
            log.error(
                "removing reference to missing album art file {}",
                util.displayable_path(old_art),
            )
            self.artpath = None
            return

        new_art = self.art_destination(old_art)
        if new_art == old_art:
            return

        new_art = util.unique_path(new_art)
        log.debug(
            "moving album art {0} to {1}",
            util.displayable_path(old_art),
            util.displayable_path(new_art),
        )
        if operation == MoveOperation.MOVE:
            util.move(old_art, new_art)
            util.prune_dirs(os.path.dirname(old_art), self._db.directory)
        elif operation == MoveOperation.COPY:
            util.copy(old_art, new_art)
        elif operation == MoveOperation.LINK:
            util.link(old_art, new_art)
        elif operation == MoveOperation.HARDLINK:
            util.hardlink(old_art, new_art)
        elif operation == MoveOperation.REFLINK:
            util.reflink(old_art, new_art, fallback=False)
        elif operation == MoveOperation.REFLINK_AUTO:
            util.reflink(old_art, new_art, fallback=True)
        else:
            assert False, "unknown MoveOperation"
        self.artpath = new_art

    def move(self, operation=MoveOperation.MOVE, basedir=None, store=True):
        """Move, copy, link or hardlink (depending on `operation`)
        all items to their destination. Any album art moves along with them.

        `basedir` overrides the library base directory for the destination.

        `operation` should be an instance of `util.MoveOperation`.

        By default, the album is stored to the database, persisting any
        modifications to its metadata. If `store` is `False` however,
        the album is not stored automatically, and it will have to be manually
        stored after invoking this method.
        """
        basedir = basedir or self._db.directory

        # Ensure new metadata is available to items for destination
        # computation.
        if store:
            self.store()

        # Move items.
        items = list(self.items())
        for item in items:
            item.move(operation, basedir=basedir, with_album=False, store=store)

        # Move art.
        self.move_art(operation)
        if store:
            self.store()

    def item_dir(self):
        """Return the directory containing the album's first item,
        provided that such an item exists.
        """
        item = self.items().get()
        if not item:
            raise ValueError("empty album for album id %d" % self.id)
        return os.path.dirname(item.path)

    def _albumtotal(self):
        """Return the total number of tracks on all discs on the album."""
        if self.disctotal == 1 or not beets.config["per_disc_numbering"]:
            return self.items()[0].tracktotal

        counted = []
        total = 0

        for item in self.items():
            if item.disc in counted:
                continue

            total += item.tracktotal
            counted.append(item.disc)

            if len(counted) == self.disctotal:
                break

        return total

    def art_destination(self, image, item_dir=None):
        """Return a path to the destination for the album art image
        for the album.

        `image` is the path of the image that will be
        moved there (used for its extension).

        The path construction uses the existing path of the album's
        items, so the album must contain at least one item or
        item_dir must be provided.
        """
        image = bytestring_path(image)
        item_dir = item_dir or self.item_dir()

        filename_tmpl = template(beets.config["art_filename"].as_str())
        subpath = self.evaluate_template(filename_tmpl, True)
        if beets.config["asciify_paths"]:
            subpath = util.asciify_path(
                subpath, beets.config["path_sep_replace"].as_str()
            )
        subpath = util.sanitize_path(
            subpath, replacements=self._db.replacements
        )
        subpath = bytestring_path(subpath)

        _, ext = os.path.splitext(image)
        dest = os.path.join(item_dir, subpath + ext)

        return bytestring_path(dest)

    def set_art(self, path, copy=True):
        """Set the album's cover art to the image at the given path.

        The image is copied (or moved) into place, replacing any
        existing art.

        Send an 'art_set' event with `self` as the sole argument.
        """
        path = bytestring_path(path)
        oldart = self.artpath
        artdest = self.art_destination(path)

        if oldart and samefile(path, oldart):
            # Art already set.
            return
        elif samefile(path, artdest):
            # Art already in place.
            self.artpath = path
            return

        # Normal operation.
        if oldart == artdest:
            util.remove(oldart)
        artdest = util.unique_path(artdest)
        if copy:
            util.copy(path, artdest)
        else:
            util.move(path, artdest)
        self.artpath = artdest

        plugins.send("art_set", album=self)

    def store(self, fields=None, inherit=True):
        """Update the database with the album information.

        `fields` represents the fields to be stored. If not specified,
        all fields will be.

        The album's tracks are also updated when the `inherit` flag is enabled.
        This applies to fixed attributes as well as flexible ones. The `id`
        attribute of the album will never be inherited.
        """
        # Get modified track fields.
        track_updates = {}
        track_deletes = set()
        for key in self._dirty:
            if inherit:
                if key in self.item_keys:  # is a fixed attribute
                    track_updates[key] = self[key]
                elif key not in self:  # is a fixed or a flexible attribute
                    track_deletes.add(key)
                elif key != "id":  # is a flexible attribute
                    track_updates[key] = self[key]

        with self._db.transaction():
            super().store(fields)
            if track_updates:
                for item in self.items():
                    for key, value in track_updates.items():
                        item[key] = value
                    item.store()
            if track_deletes:
                for item in self.items():
                    for key in track_deletes:
                        if key in item:
                            del item[key]
                    item.store()

    def try_sync(self, write, move, inherit=True):
        """Synchronize the album and its items with the database.
        Optionally, also write any new tags into the files and update
        their paths.

        `write` indicates whether to write tags to the item files, and
        `move` controls whether files (both audio and album art) are
        moved.
        """
        self.store(inherit=inherit)
        for item in self.items():
            item.try_sync(write, move)


class Item(LibModel):
    """Represent a song or track."""

    _table = "items"
    _flex_table = "item_attributes"
    _fields = {
        "id": types.PRIMARY_ID,
        "path": types.PathType(),
        "album_id": types.FOREIGN_ID,
        "title": types.STRING,
        "artist": types.STRING,
        "artists": types.MULTI_VALUE_DSV,
        "artists_ids": types.MULTI_VALUE_DSV,
        "artist_sort": types.STRING,
        "artists_sort": types.MULTI_VALUE_DSV,
        "artist_credit": types.STRING,
        "artists_credit": types.MULTI_VALUE_DSV,
        "remixer": types.STRING,
        "album": types.STRING,
        "albumartist": types.STRING,
        "albumartists": types.MULTI_VALUE_DSV,
        "albumartist_sort": types.STRING,
        "albumartists_sort": types.MULTI_VALUE_DSV,
        "albumartist_credit": types.STRING,
        "albumartists_credit": types.MULTI_VALUE_DSV,
        "genre": types.STRING,
        "style": types.STRING,
        "discogs_albumid": types.INTEGER,
        "discogs_artistid": types.INTEGER,
        "discogs_labelid": types.INTEGER,
        "lyricist": types.STRING,
        "composer": types.STRING,
        "composer_sort": types.STRING,
        "work": types.STRING,
        "mb_workid": types.STRING,
        "work_disambig": types.STRING,
        "arranger": types.STRING,
        "grouping": types.STRING,
        "year": types.PaddedInt(4),
        "month": types.PaddedInt(2),
        "day": types.PaddedInt(2),
        "track": types.PaddedInt(2),
        "tracktotal": types.PaddedInt(2),
        "disc": types.PaddedInt(2),
        "disctotal": types.PaddedInt(2),
        "lyrics": types.STRING,
        "comments": types.STRING,
        "bpm": types.INTEGER,
        "comp": types.BOOLEAN,
        "mb_trackid": types.STRING,
        "mb_albumid": types.STRING,
        "mb_artistid": types.STRING,
        "mb_artistids": types.MULTI_VALUE_DSV,
        "mb_albumartistid": types.STRING,
        "mb_albumartistids": types.MULTI_VALUE_DSV,
        "mb_releasetrackid": types.STRING,
        "trackdisambig": types.STRING,
        "albumtype": types.STRING,
        "albumtypes": types.SEMICOLON_SPACE_DSV,
        "label": types.STRING,
        "barcode": types.STRING,
        "acoustid_fingerprint": types.STRING,
        "acoustid_id": types.STRING,
        "mb_releasegroupid": types.STRING,
        "release_group_title": types.STRING,
        "asin": types.STRING,
        "isrc": types.STRING,
        "catalognum": types.STRING,
        "script": types.STRING,
        "language": types.STRING,
        "country": types.STRING,
        "albumstatus": types.STRING,
        "media": types.STRING,
        "albumdisambig": types.STRING,
        "releasegroupdisambig": types.STRING,
        "disctitle": types.STRING,
        "encoder": types.STRING,
        "rg_track_gain": types.NULL_FLOAT,
        "rg_track_peak": types.NULL_FLOAT,
        "rg_album_gain": types.NULL_FLOAT,
        "rg_album_peak": types.NULL_FLOAT,
        "r128_track_gain": types.NULL_FLOAT,
        "r128_album_gain": types.NULL_FLOAT,
        "original_year": types.PaddedInt(4),
        "original_month": types.PaddedInt(2),
        "original_day": types.PaddedInt(2),
        "initial_key": types.MusicalKey(),
        "length": types.DurationType(),
        "bitrate": types.ScaledInt(1000, "kbps"),
        "bitrate_mode": types.STRING,
        "encoder_info": types.STRING,
        "encoder_settings": types.STRING,
        "format": types.STRING,
        "samplerate": types.ScaledInt(1000, "kHz"),
        "bitdepth": types.INTEGER,
        "channels": types.INTEGER,
        "mtime": types.DATE,
        "added": types.DATE,
    }

    _search_fields = (
        "artist",
        "title",
        "comments",
        "album",
        "albumartist",
        "genre",
    )

    # Set of item fields that are backed by `MediaFile` fields.
    # Any kind of field (fixed, flexible, and computed) may be a media
    # field. Only these fields are read from disk in `read` and written in
    # `write`.
    _media_fields = set(MediaFile.readable_fields()).intersection(
        _fields.keys()
    )

    # Set of item fields that are backed by *writable* `MediaFile` tag
    # fields.
    # This excludes fields that represent audio data, such as `bitrate` or
    # `length`.
    _media_tag_fields = set(MediaFile.fields()).intersection(_fields.keys())

    _formatter = FormattedItemMapping

    _sorts = {"artist": dbcore.query.SmartArtistSort}

    @cached_classproperty
    def _queries(cls) -> dict[str, FieldQueryType]:
        return {**super()._queries, "singleton": dbcore.query.SingletonQuery}

    _format_config_key = "format_item"

    # Cached album object. Read-only.
    __album: Album | None = None

    @cached_classproperty
    def _relation(cls) -> type[Album]:
        return Album

    @cached_classproperty
    def relation_join(cls) -> str:
        """Return the FROM clause which includes related albums.

        We need to use a LEFT JOIN here, otherwise items that are not part of
        an album (e.g. singletons) would be left out.
        """
        return (
            f"LEFT JOIN {cls._relation._table} "
            f"ON {cls._table}.album_id = {cls._relation._table}.id"
        )

    @property
    def _cached_album(self):
        """The Album object that this item belongs to, if any, or
        None if the item is a singleton or is not associated with a
        library.
        The instance is cached and refreshed on access.

        DO NOT MODIFY!
        If you want a copy to modify, use :meth:`get_album`.
        """
        if not self.__album and self._db:
            self.__album = self._db.get_album(self)
        elif self.__album:
            self.__album.load()
        return self.__album

    @_cached_album.setter
    def _cached_album(self, album):
        self.__album = album

    @classmethod
    def _getters(cls):
        getters = plugins.item_field_getters()
        getters["singleton"] = lambda i: i.album_id is None
        getters["filesize"] = Item.try_filesize  # In bytes.
        return getters

    def duplicates_query(self, fields: list[str]) -> dbcore.AndQuery:
        """Return a query for entities with same values in the given fields."""
        return super().duplicates_query(fields) & dbcore.query.NoneQuery(
            "album_id"
        )

    @classmethod
    def from_path(cls, path):
        """Create a new item from the media file at the specified path."""
        # Initiate with values that aren't read from files.
        i = cls(album_id=None)
        i.read(path)
        i.mtime = i.current_mtime()  # Initial mtime.
        return i

    def __setitem__(self, key, value):
        """Set the item's value for a standard field or a flexattr."""
        # Encode unicode paths and read buffers.
        if key == "path":
            if isinstance(value, str):
                value = bytestring_path(value)
            elif isinstance(value, types.BLOB_TYPE):
                value = bytes(value)
        elif key == "album_id":
            self._cached_album = None

        changed = super()._setitem(key, value)

        if changed and key in MediaFile.fields():
            self.mtime = 0  # Reset mtime on dirty.

    def __getitem__(self, key):
        """Get the value for a field, falling back to the album if
        necessary.

        Raise a KeyError if the field is not available.
        """
        try:
            return super().__getitem__(key)
        except KeyError:
            if self._cached_album:
                return self._cached_album[key]
            raise

    def __repr__(self):
        # This must not use `with_album=True`, because that might access
        # the database. When debugging, that is not guaranteed to succeed, and
        # can even deadlock due to the database lock.
        return "{}({})".format(
            type(self).__name__,
            ", ".join(
                "{}={!r}".format(k, self[k])
                for k in self.keys(with_album=False)
            ),
        )

    def keys(self, computed=False, with_album=True):
        """Get a list of available field names.

        `with_album` controls whether the album's fields are included.
        """
        keys = super().keys(computed=computed)
        if with_album and self._cached_album:
            keys = set(keys)
            keys.update(self._cached_album.keys(computed=computed))
            keys = list(keys)
        return keys

    def get(self, key, default=None, with_album=True):
        """Get the value for a given key or `default` if it does not
        exist.

        Set `with_album` to false to skip album fallback.
        """
        try:
            return self._get(key, default, raise_=with_album)
        except KeyError:
            if self._cached_album:
                return self._cached_album.get(key, default)
            return default

    def update(self, values):
        """Set all key/value pairs in the mapping.

        If mtime is specified, it is not reset (as it might otherwise be).
        """
        super().update(values)
        if self.mtime == 0 and "mtime" in values:
            self.mtime = values["mtime"]

    def clear(self):
        """Set all key/value pairs to None."""
        for key in self._media_tag_fields:
            setattr(self, key, None)

    def get_album(self):
        """Get the Album object that this item belongs to, if any, or
        None if the item is a singleton or is not associated with a
        library.
        """
        if not self._db:
            return None
        return self._db.get_album(self)

    # Interaction with file metadata.

    def read(self, read_path=None):
        """Read the metadata from the associated file.

        If `read_path` is specified, read metadata from that file
        instead. Update all the properties in `_media_fields`
        from the media file.

        Raise a `ReadError` if the file could not be read.
        """
        if read_path is None:
            read_path = self.path
        else:
            read_path = normpath(read_path)
        try:
            mediafile = MediaFile(syspath(read_path))
        except UnreadableFileError as exc:
            raise ReadError(read_path, exc)

        for key in self._media_fields:
            value = getattr(mediafile, key)
            if isinstance(value, int):
                if value.bit_length() > 63:
                    value = 0
            self[key] = value

        # Database's mtime should now reflect the on-disk value.
        if read_path == self.path:
            self.mtime = self.current_mtime()

        self.path = read_path

    def write(self, path=None, tags=None, id3v23=None):
        """Write the item's metadata to a media file.

        All fields in `_media_fields` are written to disk according to
        the values on this object.

        `path` is the path of the mediafile to write the data to. It
        defaults to the item's path.

        `tags` is a dictionary of additional metadata the should be
        written to the file. (These tags need not be in `_media_fields`.)

        `id3v23` will override the global `id3v23` config option if it is
        set to something other than `None`.

        Can raise either a `ReadError` or a `WriteError`.
        """
        if path is None:
            path = self.path
        else:
            path = normpath(path)

        if id3v23 is None:
            id3v23 = beets.config["id3v23"].get(bool)

        # Get the data to write to the file.
        item_tags = dict(self)
        item_tags = {
            k: v for k, v in item_tags.items() if k in self._media_fields
        }  # Only write media fields.
        if tags is not None:
            item_tags.update(tags)
        plugins.send("write", item=self, path=path, tags=item_tags)

        # Open the file.
        try:
            mediafile = MediaFile(syspath(path), id3v23=id3v23)
        except UnreadableFileError as exc:
            raise ReadError(path, exc)

        # Write the tags to the file.
        mediafile.update(item_tags)
        try:
            mediafile.save()
        except UnreadableFileError as exc:
            raise WriteError(self.path, exc)

        # The file has a new mtime.
        if path == self.path:
            self.mtime = self.current_mtime()
        plugins.send("after_write", item=self, path=path)

    def try_write(self, *args, **kwargs):
        """Call `write()` but catch and log `FileOperationError`
        exceptions.

        Return `False` an exception was caught and `True` otherwise.
        """
        try:
            self.write(*args, **kwargs)
            return True
        except FileOperationError as exc:
            log.error("{0}", exc)
            return False

    def try_sync(self, write, move, with_album=True):
        """Synchronize the item with the database and, possibly, update its
        tags on disk and its path (by moving the file).

        `write` indicates whether to write new tags into the file. Similarly,
        `move` controls whether the path should be updated. In the
        latter case, files are *only* moved when they are inside their
        library's directory (if any).

        Similar to calling :meth:`write`, :meth:`move`, and :meth:`store`
        (conditionally).
        """
        if write:
            self.try_write()
        if move:
            # Check whether this file is inside the library directory.
            if self._db and self._db.directory in util.ancestry(self.path):
                log.debug(
                    "moving {0} to synchronize path",
                    util.displayable_path(self.path),
                )
                self.move(with_album=with_album)
        self.store()

    # Files themselves.

    def move_file(self, dest, operation=MoveOperation.MOVE):
        """Move, copy, link or hardlink the item depending on `operation`,
        updating the path value if the move succeeds.

        If a file exists at `dest`, then it is slightly modified to be unique.

        `operation` should be an instance of `util.MoveOperation`.
        """
        if not util.samefile(self.path, dest):
            dest = util.unique_path(dest)
        if operation == MoveOperation.MOVE:
            plugins.send(
                "before_item_moved",
                item=self,
                source=self.path,
                destination=dest,
            )
            util.move(self.path, dest)
            plugins.send(
                "item_moved", item=self, source=self.path, destination=dest
            )
        elif operation == MoveOperation.COPY:
            util.copy(self.path, dest)
            plugins.send(
                "item_copied", item=self, source=self.path, destination=dest
            )
        elif operation == MoveOperation.LINK:
            util.link(self.path, dest)
            plugins.send(
                "item_linked", item=self, source=self.path, destination=dest
            )
        elif operation == MoveOperation.HARDLINK:
            util.hardlink(self.path, dest)
            plugins.send(
                "item_hardlinked", item=self, source=self.path, destination=dest
            )
        elif operation == MoveOperation.REFLINK:
            util.reflink(self.path, dest, fallback=False)
            plugins.send(
                "item_reflinked", item=self, source=self.path, destination=dest
            )
        elif operation == MoveOperation.REFLINK_AUTO:
            util.reflink(self.path, dest, fallback=True)
            plugins.send(
                "item_reflinked", item=self, source=self.path, destination=dest
            )
        else:
            assert False, "unknown MoveOperation"

        # Either copying or moving succeeded, so update the stored path.
        self.path = dest

    def current_mtime(self):
        """Return the current mtime of the file, rounded to the nearest
        integer.
        """
        return int(os.path.getmtime(syspath(self.path)))

    def try_filesize(self):
        """Get the size of the underlying file in bytes.

        If the file is missing, return 0 (and log a warning).
        """
        try:
            return os.path.getsize(syspath(self.path))
        except (OSError, Exception) as exc:
            log.warning("could not get filesize: {0}", exc)
            return 0

    # Model methods.

    def remove(self, delete=False, with_album=True):
        """Remove the item.

        If `delete`, then the associated file is removed from disk.

        If `with_album`, then the item's album (if any) is removed
        if the item was the last in the album.
        """
        super().remove()

        # Remove the album if it is empty.
        if with_album:
            album = self.get_album()
            if album and not album.items():
                album.remove(delete, False)

        # Send a 'item_removed' signal to plugins
        plugins.send("item_removed", item=self)

        # Delete the associated file.
        if delete:
            util.remove(self.path)
            util.prune_dirs(os.path.dirname(self.path), self._db.directory)

        self._db._memotable = {}

    def move(
        self,
        operation=MoveOperation.MOVE,
        basedir=None,
        with_album=True,
        store=True,
    ):
        """Move the item to its designated location within the library
        directory (provided by destination()).

        Subdirectories are created as needed. If the operation succeeds,
        the item's path field is updated to reflect the new location.

        Instead of moving the item it can also be copied, linked or hardlinked
        depending on `operation` which should be an instance of
        `util.MoveOperation`.

        `basedir` overrides the library base directory for the destination.

        If the item is in an album and `with_album` is `True`, the album is
        given an opportunity to move its art.

        By default, the item is stored to the database if it is in the
        database, so any dirty fields prior to the move() call will be written
        as a side effect.
        If `store` is `False` however, the item won't be stored and it will
        have to be manually stored after invoking this method.
        """
        self._check_db()
        dest = self.destination(basedir=basedir)

        # Create necessary ancestry for the move.
        util.mkdirall(dest)

        # Perform the move and store the change.
        old_path = self.path
        self.move_file(dest, operation)
        if store:
            self.store()

        # If this item is in an album, move its art.
        if with_album:
            album = self.get_album()
            if album:
                album.move_art(operation)
                if store:
                    album.store()

        # Prune vacated directory.
        if operation == MoveOperation.MOVE:
            util.prune_dirs(os.path.dirname(old_path), self._db.directory)

    # Templating.

    def destination(
        self,
        relative_to_libdir=False,
        basedir=None,
        path_formats=None,
    ) -> bytes:
        """Return the path in the library directory designated for the item
        (i.e., where the file ought to be).

        The path is returned as a bytestring. ``basedir`` can override the
        library's base directory for the destination. If ``relative_to_libdir``
        is true, returns just the fragment of the path underneath the library
        base directory.
        """
        db = self._check_db()
        basedir = basedir or db.directory
        path_formats = path_formats or db.path_formats

        # Use a path format based on a query, falling back on the
        # default.
        for query, path_format in path_formats:
            if query == PF_KEY_DEFAULT:
                continue
            query, _ = parse_query_string(query, type(self))
            if query.match(self):
                # The query matches the item! Use the corresponding path
                # format.
                break
        else:
            # No query matched; fall back to default.
            for query, path_format in path_formats:
                if query == PF_KEY_DEFAULT:
                    break
            else:
                assert False, "no default path format"
        if isinstance(path_format, Template):
            subpath_tmpl = path_format
        else:
            subpath_tmpl = template(path_format)

        # Evaluate the selected template.
        subpath = self.evaluate_template(subpath_tmpl, True)

        # Prepare path for output: normalize Unicode characters.
        if sys.platform == "darwin":
            subpath = unicodedata.normalize("NFD", subpath)
        else:
            subpath = unicodedata.normalize("NFC", subpath)

        if beets.config["asciify_paths"]:
            subpath = util.asciify_path(
                subpath, beets.config["path_sep_replace"].as_str()
            )

        lib_path_str, fallback = util.legalize_path(
            subpath, db.replacements, self.filepath.suffix
        )
        if fallback:
            # Print an error message if legalization fell back to
            # default replacements because of the maximum length.
            log.warning(
                "Fell back to default replacements when naming "
                "file {}. Configure replacements to avoid lengthening "
                "the filename.",
                subpath,
            )
        lib_path_bytes = util.bytestring_path(lib_path_str)

        if relative_to_libdir:
            return lib_path_bytes

        return normpath(os.path.join(basedir, lib_path_bytes))


def _int_arg(s):
    """Convert a string argument to an integer for use in a template
    function.

    May raise a ValueError.
    """
    return int(s.strip())


class DefaultTemplateFunctions:
    """A container class for the default functions provided to path
    templates.

    These functions are contained in an object to provide
    additional context to the functions -- specifically, the Item being
    evaluated.
    """

    _prefix = "tmpl_"

    @cached_classproperty
    def _func_names(cls) -> list[str]:
        """Names of tmpl_* functions in this class."""
        return [s for s in dir(cls) if s.startswith(cls._prefix)]

    def __init__(self, item=None, lib=None):
        """Parametrize the functions.

        If `item` or `lib` is None, then some functions (namely, ``aunique``)
        will always evaluate to the empty string.
        """
        self.item = item
        self.lib = lib

    def functions(self):
        """Return a dictionary containing the functions defined in this
        object.

        The keys are function names (as exposed in templates)
        and the values are Python functions.
        """
        out = {}
        for key in self._func_names:
            out[key[len(self._prefix) :]] = getattr(self, key)
        return out

    @staticmethod
    def tmpl_lower(s):
        """Convert a string to lower case."""
        return s.lower()

    @staticmethod
    def tmpl_upper(s):
        """Convert a string to upper case."""
        return s.upper()

    @staticmethod
    def tmpl_capitalize(s):
        """Converts to a capitalized string."""
        return s.capitalize()

    @staticmethod
    def tmpl_title(s):
        """Convert a string to title case."""
        return string.capwords(s)

    @staticmethod
    def tmpl_left(s, chars):
        """Get the leftmost characters of a string."""
        return s[0 : _int_arg(chars)]

    @staticmethod
    def tmpl_right(s, chars):
        """Get the rightmost characters of a string."""
        return s[-_int_arg(chars) :]

    @staticmethod
    def tmpl_if(condition, trueval, falseval=""):
        """If ``condition`` is nonempty and nonzero, emit ``trueval``;
        otherwise, emit ``falseval`` (if provided).
        """
        try:
            int_condition = _int_arg(condition)
        except ValueError:
            if condition.lower() == "false":
                return falseval
        else:
            condition = int_condition

        if condition:
            return trueval
        else:
            return falseval

    @staticmethod
    def tmpl_asciify(s):
        """Translate non-ASCII characters to their ASCII equivalents."""
        return util.asciify_path(s, beets.config["path_sep_replace"].as_str())

    @staticmethod
    def tmpl_time(s, fmt):
        """Format a time value using `strftime`."""
        cur_fmt = beets.config["time_format"].as_str()
        return time.strftime(fmt, time.strptime(s, cur_fmt))

    def tmpl_aunique(self, keys=None, disam=None, bracket=None):
        """Generate a string that is guaranteed to be unique among all
        albums in the library who share the same set of keys.

        A fields from "disam" is used in the string if one is sufficient to
        disambiguate the albums. Otherwise, a fallback opaque value is
        used. Both "keys" and "disam" should be given as
        whitespace-separated lists of field names, while "bracket" is a
        pair of characters to be used as brackets surrounding the
        disambiguator or empty to have no brackets.
        """
        # Fast paths: no album, no item or library, or memoized value.
        if not self.item or not self.lib:
            return ""

        if isinstance(self.item, Item):
            album_id = self.item.album_id
        elif isinstance(self.item, Album):
            album_id = self.item.id

        if album_id is None:
            return ""

        memokey = self._tmpl_unique_memokey("aunique", keys, disam, album_id)
        memoval = self.lib._memotable.get(memokey)
        if memoval is not None:
            return memoval

        album = self.lib.get_album(album_id)

        return self._tmpl_unique(
            "aunique",
            keys,
            disam,
            bracket,
            album_id,
            album,
            album.item_keys,
            # Do nothing for singletons.
            lambda a: a is None,
        )

    def tmpl_sunique(self, keys=None, disam=None, bracket=None):
        """Generate a string that is guaranteed to be unique among all
        singletons in the library who share the same set of keys.

        A fields from "disam" is used in the string if one is sufficient to
        disambiguate the albums. Otherwise, a fallback opaque value is
        used. Both "keys" and "disam" should be given as
        whitespace-separated lists of field names, while "bracket" is a
        pair of characters to be used as brackets surrounding the
        disambiguator or empty to have no brackets.
        """
        # Fast paths: no album, no item or library, or memoized value.
        if not self.item or not self.lib:
            return ""

        if isinstance(self.item, Item):
            item_id = self.item.id
        else:
            raise NotImplementedError("sunique is only implemented for items")

        if item_id is None:
            return ""

        return self._tmpl_unique(
            "sunique",
            keys,
            disam,
            bracket,
            item_id,
            self.item,
            Item.all_keys(),
            # Do nothing for non singletons.
            lambda i: i.album_id is not None,
        )

    def _tmpl_unique_memokey(self, name, keys, disam, item_id):
        """Get the memokey for the unique template named "name" for the
        specific parameters.
        """
        return (name, keys, disam, item_id)

    def _tmpl_unique(
        self,
        name,
        keys,
        disam,
        bracket,
        item_id,
        db_item,
        item_keys,
        skip_item,
    ):
        """Generate a string that is guaranteed to be unique among all items of
        the same type as "db_item" who share the same set of keys.

        A field from "disam" is used in the string if one is sufficient to
        disambiguate the items. Otherwise, a fallback opaque value is
        used. Both "keys" and "disam" should be given as
        whitespace-separated lists of field names, while "bracket" is a
        pair of characters to be used as brackets surrounding the
        disambiguator or empty to have no brackets.

        "name" is the name of the templates. It is also the name of the
        configuration section where the default values of the parameters
        are stored.

        "skip_item" is a function that must return True when the template
        should return an empty string.

        "initial_subqueries" is a list of subqueries that should be included
        in the query to find the ambiguous items.
        """
        memokey = self._tmpl_unique_memokey(name, keys, disam, item_id)
        memoval = self.lib._memotable.get(memokey)
        if memoval is not None:
            return memoval

        if skip_item(db_item):
            self.lib._memotable[memokey] = ""
            return ""

        keys = keys or beets.config[name]["keys"].as_str()
        disam = disam or beets.config[name]["disambiguators"].as_str()
        if bracket is None:
            bracket = beets.config[name]["bracket"].as_str()
        keys = keys.split()
        disam = disam.split()

        # Assign a left and right bracket or leave blank if argument is empty.
        if len(bracket) == 2:
            bracket_l = bracket[0]
            bracket_r = bracket[1]
        else:
            bracket_l = ""
            bracket_r = ""

        # Find matching items to disambiguate with.
        query = db_item.duplicates_query(keys)
        ambigous_items = (
            self.lib.items(query)
            if isinstance(db_item, Item)
            else self.lib.albums(query)
        )

        # If there's only one item to matching these details, then do
        # nothing.
        if len(ambigous_items) == 1:
            self.lib._memotable[memokey] = ""
            return ""

        # Find the first disambiguator that distinguishes the items.
        for disambiguator in disam:
            # Get the value for each item for the current field.
            disam_values = {s.get(disambiguator, "") for s in ambigous_items}

            # If the set of unique values is equal to the number of
            # items in the disambiguation set, we're done -- this is
            # sufficient disambiguation.
            if len(disam_values) == len(ambigous_items):
                break
        else:
            # No disambiguator distinguished all fields.
            res = f" {bracket_l}{item_id}{bracket_r}"
            self.lib._memotable[memokey] = res
            return res

        # Flatten disambiguation value into a string.
        disam_value = db_item.formatted(for_path=True).get(disambiguator)

        # Return empty string if disambiguator is empty.
        if disam_value:
            res = f" {bracket_l}{disam_value}{bracket_r}"
        else:
            res = ""

        self.lib._memotable[memokey] = res
        return res

    @staticmethod
    def tmpl_first(s, count=1, skip=0, sep="; ", join_str="; "):
        """Get the item(s) from x to y in a string separated by something
        and join then with something.

        Args:
            s: the string
            count: The number of items included
            skip: The number of items skipped
            sep: the separator. Usually is '; ' (default) or '/ '
            join_str: the string which will join the items, default '; '.
        """
        skip = int(skip)
        count = skip + int(count)
        return join_str.join(s.split(sep)[skip:count])

    def tmpl_ifdef(self, field, trueval="", falseval=""):
        """If field exists return trueval or the field (default)
        otherwise, emit return falseval (if provided).

        Args:
            field: The name of the field
            trueval: The string if the condition is true
            falseval: The string if the condition is false

        Returns:
            The string, based on condition.
        """
        if field in self.item:
            return trueval if trueval else self.item.formatted().get(field)
        else:
            return falseval
