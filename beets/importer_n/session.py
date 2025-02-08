from dataclasses import dataclass
import enum
import logging
from abc import ABC, abstractmethod
from typing import Literal, Optional, Union

from beets import library
from beets.autotag import AlbumMatch, TrackMatch
from beets.util import MoveOperation

import logging

log = logging.getLogger("beets")


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


Match = Union[AlbumMatch, TrackMatch]


@dataclass(slots=True)
class ImportSessionConfig:
    # If the session should resume from a previous state.
    resume: bool | Literal["ask"] = False
    incremental: bool = False

    # How the session should write
    manipulate_operation: MoveOperation = MoveOperation.NONE
    # Wether to delete the source files after the operation.
    _manipulate_delete_after: bool = False

    @property
    def manipulate_delete_after(self):
        """Wether to delete the source files after the operation.
        Can only be True if manipulate_operation is COPY.
        """
        if not self.manipulate_operation == MoveOperation.COPY:
            return False
        return self._manipulate_delete_after

    @classmethod
    def from_config(cls, config):
        """
        Set `config` property from global import config and make
        implied changes.

        Usage
        -----
        ```
        from beets import config
        ImportSessionConfig.from_config(config['import'])
        ```
        """
        if config is None:
            log.warning("No import configuration found. Using defaults.")
            return cls()
        config = dict(config)

        # Parse operation
        oper = MoveOperation.NONE
        if config["move"]:
            oper = MoveOperation.MOVE
        elif config["link"]:
            oper = MoveOperation.LINK
        elif config["copy"]:
            oper = MoveOperation.COPY
        elif config["hardlink"]:
            oper = MoveOperation.HARDLINK
        elif config["reflink"]:
            reflink = config["reflink"].as_choice(["auto", True, False])
            if reflink == "auto":
                oper = MoveOperation.REFLINK_AUTO
            elif reflink:
                oper = MoveOperation.REFLINK

        # Parse resume and incremental of session
        delete_after = resume = incremental = False
        if config["resume"]:
            resume = True
        # Incremental and progress are mutually exclusive.
        if config["incremental"]:
            incremental = True
            resume = False
        if config["delete"]:
            delete_after = True

        return cls(
            resume=resume,
            incremental=incremental,
            manipulate_operation=oper,
            _manipulate_delete_after=delete_after,
        )


class ImportSession(ABC):
    """Controls an import action. Subclasses should implement methods to
    communicate with the user or otherwise make decisions.

    This base class has implementations for the run() method but some abstract
    methods that must be implemented by subclasses. See e.g. TerminalImportSession
    for an example implementation.
    """

    lib: library.Library
    logger: logging.Logger

    def __init__(
        self,
        lib: library.Library,
        log_handler: Optional[logging.Handler] = None,
    ) -> None:
        """Create a new import session.

        Parameters
        ----------
        lib : Library
            The library instance to which items will be imported.
        log_handler : Optional[logging.Handler]
            A logging handler to use for the session's logger. If None, a
            NullHandler will be used.
        """

        self.lib = lib
        self.logger = self._setup_logging(log_handler)

    def _setup_logging(self, log_handler: Optional[logging.Handler] = None):
        logger = logging.getLogger(__name__)
        logger.propagate = False
        if not log_handler:
            log_handler = logging.NullHandler()
        logger.handlers = [log_handler]
        return logger

    @abstractmethod
    def choose_match(self, task) -> UserAction | Match:
        """Choose a match for a task.

        This method should return a Match object or None to skip the task.
        """
        pass

    def run(self):
        pass

    def __call__(self):
        """Run the import session."""
        return self.run()
