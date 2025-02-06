import asyncio
import functools
import inspect
from concurrent.futures import Executor
from dataclasses import dataclass
from typing import (
    Any,
    AsyncGenerator,
    AsyncIterable,
    Callable,
    Generator,
    Generic,
    Iterable,
    Optional,
    Union,
)

from typing_extensions import Protocol, TypeVar, TypeVarTuple, Unpack

# Typing
R = TypeVar("R", bound=Any)


Task = TypeVar("Task")
Returns = Union[
    Iterable[Task],
    AsyncIterable[Task],
]
Args = TypeVarTuple("Args")
Return = TypeVar("Return", bound=Returns, covariant=True)
StageFunc = Callable[[Unpack[Args], Task], Return]


class Sentinel:
    __slots__ = ()


class StageI(Protocol[Task]):
    """Abstract interface for a stage in the pipeline."""

    func: Callable

    async def collect_in_queue(
        self,
        task: Task,
        queue: asyncio.Queue[Task | Sentinel],
        executor: Optional[Executor],
    ): ...


@dataclass(slots=True)
class Stage(Generic[Task, Return, Unpack[Args]], StageI[Task]):
    """A stage processes tasks in the pipeline.

    An stage in the abstract sense can be anything that can be called with a task
    and yields new tasks. We want the following call signatures to be supported:

    Generics
    --------
    - Task: The type of the tasks that are produced.
    - Args: The type of the parameters that are passed to the stage.
    - Return: The type of the tasks that are produced.

    Function call signatures
    ------------------------
    - (*args, task: Task) -> Iterable[Task]
    - (*args, task: Task) -> Generator[Task, None, None]
    - async (*args, task: Task) -> AsyncGenerator[Task, None]
    - async (*args, task: Task) -> AsyncIterable[Task]

    Note: In theory the input and output tasks can be different types but typing
    for this in python is very chunky and difficult to implement in a generic way.
    """

    func: StageFunc[Unpack[Args], Task, Return]
    args: tuple  # P.args: tuple (not possible to type hint)

    def as_callable(self) -> Callable[[Task], Return]:
        """Return the stage as a callable."""
        return functools.partial(self.func, *self.args)

    async def collect_in_queue(
        self,
        task: Task,
        queue: asyncio.Queue[Task | Sentinel],
        executor: Optional[Executor],
    ):
        """Collect the results of the stage in a queue.

        If the function isn't async we run it in the executor.
        """
        f = self.as_callable()
        if inspect.isasyncgenfunction(f):
            async for new_task in f(task):
                await queue.put(new_task)
        else:
            if executor is not None:
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(
                    executor,
                    _helper_queue,
                    queue,
                    f,
                    task,
                )
            else:
                _helper_queue(queue, f, task)


@dataclass(slots=True)
class MutatorStage(Generic[Task, Unpack[Args]], StageI[Task]):
    """A stage that mutates the task in place.

    This stage is useful when the task is a mutable object and we want to
    modify it in place. This is particularly useful when the task is a
    dictionary or a dataclass.

    Generics
    --------
    - Task: The type of the tasks that are produced.
    - Args: The type of the parameters that are passed to the stage.

    Function call signatures
    ------------------------
    - (*args, task) -> None
    - async (*args, task) -> None
    """

    func: Callable[[Unpack[Args], Task], Any]
    args: tuple

    def as_callable(self) -> Callable[[Task], Any]:
        """Return the stage as a callable."""
        return functools.partial(self.func, *self.args)

    async def collect_in_queue(
        self,
        task: Task,
        queue: asyncio.Queue[Task | Sentinel],
        executor: Optional[Executor],
    ):
        """Collect the results of the stage in a queue.

        If the function isn't async we run it in the executor.
        """
        f = self.as_callable()
        if inspect.iscoroutinefunction(f):
            await f(task)
            await queue.put(task)
        else:
            if executor is not None:
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(executor, _helper_mutator, queue, f, task)
            else:
                _helper_mutator(queue, f, task)


@dataclass(slots=True)
class Producer(Generic[Task, Return, Unpack[Args]]):
    """A producer generates tasks for the pipeline.

    We also allow to pass an iterable of tasks as a producer.
    args and kwargs are none if the producer is a generator and
    not a callable.

    Generics
    --------
    - Task: The type of the tasks that are produced.
    - Params: The type of the parameters that are passed to the producer.
    - Return: The type of the tasks that are produced.

    Function call signatures
    ------------------------
    - (*args) -> Iterable[Task]
    - (*args) -> Generator[Task, None, None]
    - async (*args) -> AsyncIterable[Task]
    - async (*args) -> AsyncGenerator[Task, None]

    Note: You may also pass an iterable directly as a producer.
    """

    func: Callable[[Unpack[Args]], Return] | Return
    args: Optional[tuple]  # P.args: tuple (not possible to type hint)

    async def _as_async_iterable(self) -> AsyncIterable[Task]:
        """Return the results of the producer as an iterable."""
        if isinstance(self.func, (Iterable, Generator)):
            for task in self.func:
                yield task
        elif isinstance(self.func, (AsyncIterable, AsyncGenerator)):
            async for task in self.func:
                yield task
        else:
            assert False, "Producer must be an iterable, generator, or async generator."

    async def collect_in_queue(
        self, queue: asyncio.Queue[Task | Sentinel], executor: Optional[Executor]
    ):
        """Collect the results of the producer in a queue.

        This function is used to collect the results of the producer in a queue.
        """
        if isinstance(self.func, Callable):
            f = functools.partial(self.func, *self.args)
            if inspect.isasyncgenfunction(f) or inspect.isasyncgenfunction(
                self.func.__call__  # type: ignore allows to run classes with callables
            ):
                async for task in f():  # type: ignore
                    await queue.put(task)
            else:
                loop = asyncio.get_running_loop()
                # If executor is provided we run the function in the executor
                if executor is not None:
                    await loop.run_in_executor(executor, _helper_queue, queue, f)
                else:  # else we run it in the main thread. Might be blocking!
                    _helper_queue(queue, f)
        else:
            async for task in self._as_async_iterable():
                await queue.put(task)


def _helper_queue(queue: asyncio.Queue[Task | Sentinel], f, *args):
    for task in f(*args):
        queue.put_nowait(task)


def _helper_mutator(queue: asyncio.Queue[Task | Sentinel], f, task: Task):
    f(task)
    queue.put_nowait(task)
