from __future__ import annotations

import enum
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Iterable, Optional, Sequence

from beets.importer import ImportTask

if TYPE_CHECKING:
    from beets import autotag, library
    from beets.util import PathLike


class UserAction(enum.Enum):
    NONE = 0
    SKIP = 1
    ASIS = 2
    AS_TRACKS = 3
    APPLY = 4
    AS_ALBUMS = 5

    # The RETAG action represents "don't apply any match, but do record
    # new metadata". It's not reachable via the standard command prompt but
    # can be used by plugins.
    RETAG = 6


class BaseImportTask(ABC):
    """An abstract base class for importer tasks."""

    toppath: Optional[PathLike]
    paths: list[PathLike]
    items: list[library.Item]

    def __init__(
        self,
        toppath: Optional[PathLike],
        paths: Optional[Iterable[PathLike]],
        items: Optional[Iterable[library.Item]],
    ):
        """Create a task.

        The task is a unit of work for the importer. It represents a
        set of items to be imported along with their intermediate state.

        Parameters
        ----------
        toppath : Optional[PathLike]
            The user-specified base directory that contains the music for
            this task. If the task has *no* user-specified base (for example,
            when importing based on an -L query), this can be None. This is
            used for tracking progress and history.
        paths : Optional[Iterable[PathLike]]
            A list of *specific* paths where the music for this task came
            from. These paths can be directories, when their entire contents
            are being imported, or files, when the task comprises individual
            tracks. This is used for progress/history tracking and for
            displaying the task to the user.
        items : Optional[Iterable[library.Item]]
            A list of `Item` objects representing the music being imported.
            This is used for progress/history tracking and for displaying the
            task to the user.
        """
        self.toppath = toppath
        self.paths = list(paths) if paths is not None else []
        self.items = list(items) if items is not None else []

    @abstractmethod
    def handle_created():
        """Handle the task creation."""
        pass

    @abstractmethod
    def lookup_candidates(self, search_ids: Optional[Sequence[str]] = None):
        """Retrieve and store candidates for this task (album/item).

        Parameters
        ----------
        search_ids : Optional[Sequence[str]], optional
            A list of candidate IDs to restrict the initial lookup to. If
            None, the lookup is unrestricted.

        """
        pass

    @abstractmethod
    def choose_match(self, task: ImportTask) -> UserAction | Match:
        """Choose a match from the candidates.

        Make a choice about the match for this task. This method should
        return a `UserAction` enum value, an `AlbumMAtch` or `ItemMatch`
        object directly.
        """
        pass

    @abstractmethod
    def manipulate_files(self, operation):
        """Manipulate the files based on the user choice."""
        pass

    # ------------------------------- Some helpers ------------------------------- #

    @property
    def apply(self) -> bool:
        """Return True if the current choice is to apply the metadata."""
        return self.current_choice == Action.APPLY

    @property
    def skip(self) -> bool:
        """Return True if the current choice is to skip the task."""
        return self.current_choice == Action.SKIP
