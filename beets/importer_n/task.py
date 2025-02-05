from __future__ import annotations

import enum
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Iterable, Optional, TypeVar

if TYPE_CHECKING:
    from beets import autotag, library
    from beets.util import PathLike


class Action(enum.Enum):
    NONE = 0
    SKIP = 1
    ASIS = 2
    TRACKS = 3
    APPLY = 4
    ALBUMS = 5

    # The RETAG action represents "don't apply any match, but do record
    # new metadata". It's not reachable via the standard command prompt but
    # can be used by plugins.
    RETAG = 6


class BaseTask(ABC):
    """An abstract base class for importer tasks.

    Tasks flow through the importer pipeline. Each stage can update
    them.
    """

    toppath: Optional[PathLike]
    paths: list[PathLike]
    items: list[library.Item]

    # We keep track of the current user choice
    # for the task. This may be used to make
    # some decisions in the pipeline.
    current_choice: Action = Action.NONE
    current_match: Optional[autotag.AlbumMatch | autotag.TrackMatch] = None

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
    def lookup_candidates(self):
        """Find candidate matches for the task."""
        pass

    @abstractmethod
    def choose_match(self) -> Action:
        """Choose a match from the candidates."""
        pass

    def set_current(
        self,
        choice: Optional[Action],
        match: Optional[autotag.AlbumMatch | autotag.TrackMatch],
    ):
        """Set the user choice for the task.

        Also, set the match that the user has chosen if any. Atm this implies
        that the user has made a apply choice.

        Parameters
        ----------
        choice : Optional[Action]
            The user choice for the task. If None, the choice is not set.
        match : Optional[autotag.AlbumMatch| autotag.TrackMatch]
            The match that the user has chosen.
        """
        if choice is None:
            choice = Action.NONE

        if match is not None:
            self.current_match = match
        else:
            # Implicit choice.
            self.current_choice = Action.APPLY
            self.current_match = match

    # ------------------------------- Some helpers ------------------------------- #

    @property
    def apply(self) -> bool:
        """Return True if the current choice is to apply the metadata."""
        return self.current_choice == Action.APPLY

    @property
    def skip(self) -> bool:
        """Return True if the current choice is to skip the task."""
        return self.current_choice == Action.SKIP
