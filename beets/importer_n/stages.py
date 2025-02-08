"""Stages are the steps taken to import a album or track into the library.

Might include blocking or async functions. The pipeline is a collection of
function which are basically chained together.
"""

from __future__ import annotations

import itertools
import logging
from typing import TYPE_CHECKING, AsyncIterable, Callable, Iterable, Sequence

from beets import config, library, plugins
from beets.importer_n import AsyncPipeline
from .session import ImportSession
from beets.util import PathLike, displayable_path

if TYPE_CHECKING:
    from beets.importer import (
        ImportSession as ImportSessionOld,
        ImportTask,
        ImportTaskFactory,
        SentinelImportTask,
        SingletonImportTask,
        action,
    )

log = logging.getLogger("beets")


# ---------------------------- Producer functions ---------------------------- #
# Functions that are called first i.e. they generate import tasks


def read_tasks(
    session: ImportSession, paths: Sequence[PathLike]
) -> Iterable[ImportTask]:
    """A generator yielding all the albums (as ImportTask objects) found
    in the user-specified list of paths. In the case of a singleton
    import, yields single-item tasks instead.
    """
    skipped = 0
    if len(paths) == 0:
        log.warning("No path specified in session.")
        return

    for toppath in paths:
        # Check whether we need to resume the import.
        session.ask_resume(toppath)

        # Generate tasks.
        task_factory = ImportTaskFactory(toppath, session)
        yield from task_factory.tasks()
        skipped += task_factory.skipped

        if not task_factory.imported:
            log.warning("No files imported from {0}", displayable_path(toppath))

    # Show skipped directories (due to incremental/resume).
    if skipped:
        log.info("Skipped {0} paths.", skipped)


def query_tasks(session: ImportSession) -> Iterable[ImportTask]:
    """A generator that works as a drop-in-replacement for read_tasks.
    Instead of finding files from the filesystem, a query is used to
    match items from the library.
    """
    if session.config["singletons"]:
        # Search for items.
        for item in session.lib.items(session.query):
            task = SingletonImportTask(None, item)
            for task in task.handle_created(session):
                yield task

    else:
        # Search for albums.
        for album in session.lib.albums(session.query):
            log.debug(
                "yielding album {0}: {1} - {2}",
                album.id,
                album.albumartist,
                album.album,
            )
            items = list(album.items())
            _freshen_items(items)

            task = ImportTask(None, [album.item_dir()], items)
            for task in task.handle_created(session):
                yield task


# ---------------------------------- Stages ---------------------------------- #
# Functions that process import tasks, may transform or filter them
# They are chained together in the pipeline e.g. stage2(stage1(task)) -> task


def group_albums(
    session: ImportSession,
    task: ImportTask,
) -> Iterable[ImportTask]:
    """Groups are identified using their artist and album fields. The
    pipeline stage emits new album tasks for each discovered group.
    """

    if task.skip:
        # FIXME This gets duplicated a lot. We need a better
        # abstraction.
        return

    def group(item):
        return (item.albumartist or item.artist, item.album)

    tasks = []
    sorted_items = sorted(task.items, key=group)
    for _, items in itertools.groupby(sorted_items, group):
        items = list(items)
        task = ImportTask(task.toppath, [i.path for i in items], items)
        tasks += task.handle_created(session)
    tasks.append(SentinelImportTask(task.toppath, task.paths))

    yield from tasks


def lookup_candidates(
    session: ImportSession,
    task: ImportTask,
) -> Iterable[ImportTask]:
    if task.skip:
        return

    plugins.send("import_task_start", session=session, task=task)
    log.debug("Looking up: {0}", displayable_path(task.paths))

    # Restrict the initial lookup to IDs specified by the user via the -m
    # option. Currently all the IDs are passed onto the tasks directly.
    task.search_ids = session.config["search_ids"].as_str_seq()

    task.lookup_candidates()

    yield task


async def user_query(
    session: ImportSession,
    task: ImportTask,
) -> AsyncIterable[ImportTask]:
    if task.skip:
        return

    if session.already_merged(task.paths):
        return

    # Ask the user for a choice.
    task.choose_match(session)
    plugins.send("import_task_choice", session=session, task=task)

    # As-tracks: transition to singleton workflow.
    if task.choice_flag is action.TRACKS:
        # Set up a little pipeline for dealing with the singletons.
        def emitter(task):
            for item in task.items:
                task = SingletonImportTask(task.toppath, item)
                yield from task.handle_created(session)
            yield SentinelImportTask(task.toppath, task.paths)
            yield task

        # Create an additional pipeline and chain it to the existing one
        pl = AsyncPipeline()
        pl.set_producer(emitter, task)
        pl.add_mutator(lookup_candidates, session)
        pl.add_stage(user_query, session)
        async for t in pl():
            yield t
        return

    # As albums: group items by albums and create task for each album
    if task.choice_flag is action.ALBUMS:
        pl = AsyncPipeline()
        pl.set_producer([task])
        pl.add_mutator(group_albums, session)
        pl.add_mutator(lookup_candidates, session)
        pl.add_stage(user_query, session)
        async for t in pl():
            yield t
        return

    _resolve_duplicates(session, task)

    if task.should_merge_duplicates:
        # Create a new task for tagging the current items
        # and duplicates together
        duplicate_items = task.duplicate_items(session.lib)

        # Duplicates would be reimported so make them look "fresh"
        _freshen_items(duplicate_items)
        duplicate_paths = [item.path for item in duplicate_items]

        # Record merged paths in the session so they are not reimported
        session.mark_merged(duplicate_paths)

        merged_task = ImportTask(
            None, task.paths + duplicate_paths, task.items + duplicate_items
        )

        pl = AsyncPipeline()
        pl.set_producer([merged_task])
        pl.add_mutator(lookup_candidates, session)
        pl.add_stage(user_query, session)
        async for t in pl():
            yield t
        return

    _apply_choice(session, task)
    yield task


def import_asis(
    session: ImportSession,
    task: ImportTask,
) -> Iterable[ImportTask]:
    """Select the `action.ASIS` choice for all tasks.

    This stage replaces the initial_lookup and user_query stages
    when the importer is run without autotagging.
    """
    if task.skip:
        return

    log.info("{}", displayable_path(task.paths))
    task.set_choice(action.ASIS)
    _apply_choice(session, task)
    yield task


def plugin_stage(
    session: ImportSession,
    plugin_func: Callable[[ImportSession, ImportTask], None],
    task: ImportTask,
) -> Iterable[ImportTask]:
    """Allows plugins to be add to the pipeline.

    This stage is added for each plugin. These stages than occur between
    applying metadata changes and moving/copying/writing files.
    """
    if task.skip:
        return

    plugin_func(session, task)

    # Stage may modify DB, so re-load cached item data.
    # FIXME Importer plugins should not modify the database but instead
    # the albums and items attached to tasks.
    task.reload()
    yield task


async def log_tasks(
    task: ImportTask,
) -> AsyncIterable[ImportTask]:
    """Log each task. Mainly used in testing."""
    if isinstance(task, SingletonImportTask):
        log.info("Singleton: {0}", displayable_path(task.item["path"]))
    elif task.items:
        log.info("Album: {0}", displayable_path(task.paths[0]))
        for item in task.items:
            log.info("  {0}", displayable_path(item["path"]))
    yield task


# --------------------------------- Consumer --------------------------------- #
# Anything that should be placed last in the pipeline
# In theory every stage could be a consumer, but in practice there are some
# functions which are typically placed last in the pipeline


def manipulate_files(
    session: ImportSession,
    task: ImportTask,
) -> Iterable[ImportTask]:
    """Performs necessary file manipulations *after*
    items have been added to the library and
    finalizes each task.

    I.e. Moves, Copies, Links, Deletes files...

    Yields the task after it has been finalized mainly for testing purposes.
    """
    if not task.skip:
        if task.should_remove_duplicates:
            task.remove_duplicates(session.lib)

        task.manipulate_files(
            session.manipulate_files_operation,
            write=session.config["write"],
            session=session,
        )

    # Progress, cleanup, and event.
    task.finalize(session)
    yield task


# ---------------------------- Utility functions ----------------------------- #


def _freshen_items(items: Sequence[library.Item]):
    # Clear IDs from re-tagged items so they appear "fresh" when
    # we add them back to the library.
    for item in items:
        item.id = None
        item.album_id = None


def _apply_choice(
    session: ImportSession,
    task: ImportTask,
):
    """Apply the task's choice to the Album or Item it contains and add
    it to the library.
    """
    if task.skip:
        return

    # Change metadata.
    if task.apply:
        task.apply_metadata()
        plugins.send("import_task_apply", session=session, task=task)

    task.add(session.lib)

    # If ``set_fields`` is set, set those fields to the
    # configured values.
    # NOTE: This cannot be done before the ``task.add()`` call above,
    # because then the ``ImportTask`` won't have an `album` for which
    # it can set the fields.
    if config["import"]["set_fields"]:
        task.set_fields(session.lib)


def _resolve_duplicates(
    session: ImportSession,
    task: ImportTask,
):
    """Check if a task conflicts with items or albums already imported
    and ask the session to resolve this.
    """
    if task.choice_flag in (action.ASIS, action.APPLY, action.RETAG):
        found_duplicates = task.find_duplicates(session.lib)
        if found_duplicates:
            log.debug(
                "found duplicates: {}".format([o.id for o in found_duplicates])
            )

            # Get the default action to follow from config.
            duplicate_action = config["import"]["duplicate_action"].as_choice(
                {
                    "skip": "s",
                    "keep": "k",
                    "remove": "r",
                    "merge": "m",
                    "ask": "a",
                }
            )
            log.debug("default action for duplicates: {0}", duplicate_action)

            if duplicate_action == "s":
                # Skip new.
                task.set_choice(action.SKIP)
            elif duplicate_action == "k":
                # Keep both. Do nothing; leave the choice intact.
                pass
            elif duplicate_action == "r":
                # Remove old.
                task.should_remove_duplicates = True
            elif duplicate_action == "m":
                # Merge duplicates together
                task.should_merge_duplicates = True
            else:
                # No default action set; ask the session.
                session.resolve_duplicate(task, found_duplicates)

            session.log_choice(task, True)
