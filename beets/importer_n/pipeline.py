"""
The pipeline processes tasks in stages.

A pipeline allows to define how tasks are processed in a series of
stages. Each stage may allow to yield more tasks which get queued
for the next stage.

Example:
Lets assume we have a pipeline with the following stages:
producer -> stage1 -> stage2 -> consumer
                   -> stage2 -> consumer
Here stage1 may yield multiple tasks which need to get
processed by stage2.

Notation:
- **producer**: the first stage in the pipeline, generates the initial tasks.
- **consumer**: the last stage in the pipeline, consume the final tasks and
doesn't yield new tasks.
"""

from __future__ import annotations

import asyncio
import functools
import inspect
import logging
import time
from abc import ABC
from concurrent.futures import Executor, ThreadPoolExecutor
from dataclasses import dataclass
from typing import (
    Any,
    AsyncGenerator,
    AsyncIterable,
    Callable,
    Concatenate,
    Generator,
    Generic,
    Iterable,
    Mapping,
    Optional,
    ParamSpec,
    TypeVar,
    Union,
)

log = logging.getLogger("beets")

# ---------------------------------- Stages ---------------------------------- #
# Typing
R = TypeVar("R", bound=Any)


Task = TypeVar("Task")
Returns = Union[
    Iterable[Task],
    AsyncIterable[Task],
]
Params = ParamSpec("Params")
Return = TypeVar("Return", bound=Returns)
StageFunc = Callable[Concatenate[Task, Params], Return]


@dataclass(slots=True)
class Stage(Generic[Task, Params, Return]):
    """An stage in the abstract sense can be anything that can be called with a task
    and yields new tasks. We want the following call signatures to be supported:

    **sync**:
    (task: Task, *args, **kwargs) -> Iterable[Task]
    (task: Task, *args, **kwargs) -> Generator[Task, None, None]
    **async**:
    async (task: Task, *args, **kwargs) -> AsyncGenerator[Task, None]
    async (task: Task, *args, **kwargs) -> AsyncIterable[Task]
    **special case**:
    (task: Task, *args, **kwargs) -> None

    Note: In theory the input and output tasks can be different types but typing
    for this in python is very chunky and difficult to implement in a generic way.
    """

    func: StageFunc[Task, Params, Return]
    args: tuple  # P.args: tuple (not possible to type hint)
    kwargs: Mapping[str, Any]  # P.kwargs: dict (not possible to type hint)

    def as_callable(self, task: Task):
        """Return a partial function with the arguments and keyword arguments
        set.
        """
        return functools.partial(self.func, task, *self.args, **self.kwargs)

    async def collect_in_queue(
        self,
        task: Task,
        queue: asyncio.Queue[Task | Sentinel],
        executor: Executor,
    ):
        """Collect the results of the stage in a queue.
        If the function isn't async we run it in the executor.
        """
        if inspect.isasyncgenfunction(self.func):
            async for new_task in self.func(task, *self.args, **self.kwargs):
                await queue.put(new_task)
        else:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(
                executor,
                _helper_queue,
                queue,
                self.func,
                task,
                *self.args,
                **self.kwargs,
            )


@dataclass(slots=True)
class Producer(Generic[Task, Params, Return]):
    """A producer generates tasks for the pipeline.

    We also allow to pass an iterable of tasks as a producer.
    args and kwargs are none if the producer is a generator and
    not a callable.

    Generics:
    - Task: The type of the tasks that are produced.
    - Params: The type of the parameters that are passed to the producer.
    - Return: The type of the tasks that are produced.
    """

    func: Callable[Params, Return] | Return
    args: Optional[tuple]  # P.args: tuple (not possible to type hint)
    kwargs: Optional[Mapping[str, Any]]  # P.kwargs: dict (not possible to type hint)

    def as_callable(self):
        """Return a partial function with the arguments and keyword arguments
        set.

        Returns none if the producer isn't a callable.
        """
        if isinstance(self.func, Callable):
            return functools.partial(self.func, *self.args, **self.kwargs or {})

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
        self, queue: asyncio.Queue[Task | Sentinel], executor: Executor
    ):
        """Collect the results of the producer in a queue.

        This function is used to collect the results of the producer in a queue.
        """
        if isinstance(self.func, Callable):
            f = functools.partial(self.func, *self.args, **self.kwargs or {})
            if inspect.isasyncgenfunction(f) or inspect.isasyncgenfunction(
                self.func.__call__  # type: ignore allows to run classes with callables
            ):
                async for task in f():  # type: ignore
                    await queue.put(task)
            else:
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(executor, _helper_queue, queue, f)
        else:
            async for task in self._as_async_iterable():
                await queue.put(task)


def _helper_queue(queue: asyncio.Queue[Task | Sentinel], f, *args):
    for task in f(*args):
        queue.put_nowait(task)


# --------------------------------- Pipelines -------------------------------- #
class Pipeline(ABC, Generic[Task, Return]):
    """A pipeline of stages that process tasks."""

    producer: Producer[Task, ..., Return]
    stages: list[Stage[Task, ..., Return]]

    def __init__(self):
        self.stages = []

    def set_producer(
        self,
        func: Callable[Params, Return] | Return,
        *args: Params.args,
        **kwargs: Params.kwargs,
    ) -> None:
        """Set the producer of the pipeline."""
        self.producer = Producer(func, args, kwargs)

    def add_stage(
        self,
        func: StageFunc[Task, Params, Return],
        *args: Params.args,
        **kwargs: Params.kwargs,
    ) -> None:
        """Add a stage to the pipeline."""
        self.stages.append(Stage(func, args, kwargs))

    def add_stages(
        self,
        *funcs: StageFunc[Task, Params, Return],
    ):
        """Add multiple stages to the pipeline."""
        for func in funcs:
            self.stages.append(Stage(func, (), {}))

    @property
    def consumer(self) -> Stage[Task, ..., Return]:
        return self.stages[-1]


class Sentinel:
    __slots__ = ()


class AsyncPipeline(Pipeline[Task, Returns[Task]]):
    """An asynchronous pipeline of stages that process tasks.

    Using an asynchronous pipeline allows for the concurrent processing of tasks
    without blocking the execution of other tasks. This is particularly beneficial
    when dealing with I/O-bound operations, as it permits other tasks to
    progress while waiting for external resources.

    Possible todos:
    - At the moment this just runs forever if any producer
    or stage doesn't yield a result. We could add a timeout to make it
    more resilient.
    """

    # Pass tasks between stages using queues
    queues: list[asyncio.Queue[Task | Sentinel]]
    # Sentinel value to indicate the end of the pipeline
    sentinel = Sentinel()
    # Executor for running tasks
    executor: Executor

    def __init__(self, executor: Optional[Executor] = None):
        super().__init__()

        self.executor = executor or ThreadPoolExecutor()
        if not isinstance(self.executor, (ThreadPoolExecutor)):
            raise ValueError(
                """Executor must be a ThreadPoolExecutor.
                Other executors are not supported yet."""
            )

    async def collect_results(self):
        """Collect the results from the consumer stage."""
        res = []
        async for r in self():
            res.append(r)
        return res

    async def __call__(self) -> AsyncIterable[Task]:
        """Run the pipeline asynchronously."""

        self.queues = [asyncio.Queue() for _ in range(len(self.stages) + 1)]

        coros = (
            asyncio.create_task(self._run_producer()),
            asyncio.create_task(self._run_stages()),
        )
        while True:
            res = await self.queues[-1].get()
            if isinstance(res, (Sentinel, Exception)):
                break
            yield res
        potential_exceptions = await asyncio.gather(*coros, return_exceptions=True)
        # Parse exceptions as values to prevent cancellation errors
        # We should use  a TaskGroup once we migrate to 3.11
        for pe in potential_exceptions:
            if isinstance(pe, Exception):
                raise pe

    async def _run_producer(self):
        """Produce tasks for the pipeline and enqueue them."""

        # Parse possible producer types
        try:
            # Run task in executor
            await self.producer.collect_in_queue(self.queues[0], self.executor)
            # Signal the end of the pipeline
            await self.queues[0].put(self.sentinel)
        except Exception as e:
            for q in self.queues:
                await q.put(self.sentinel)
            raise e

    async def _run_stages(self):
        """Process tasks in the pipeline stages.

        We create a task for each (stage, task) pair and pass the results
        to the next stage on completion. This allows for concurrent processing
        of tasks in the pipeline.
        """
        coros = []
        for i, stage in enumerate(self.stages):  # Skip the consumer
            coros.append(self._run_stage(i, stage))
        await asyncio.gather(*coros)

    async def _run_stage(self, i: int, stage: Stage[Task, Params, Returns[Task]]):
        """
        Creates asyncio coroutines for each task in the current queue and
        enqueues the results for the next stage. Stops when the sentinel is
        reached.
        """

        time_start = time.time()
        loop = asyncio.get_running_loop()

        async def exec_task(task: Task, stage: Stage[Task, Params, Returns[Task]]):
            # Run task in executor
            try:
                await stage.collect_in_queue(task, self.queues[i + 1], self.executor)
            except Exception as e:
                for q in self.queues:
                    await q.put(self.sentinel)
                raise e

        stage_coros = []
        while True:
            task = await self.queues[i].get()
            if isinstance(task, (Sentinel, Exception)):
                break
            stage_coros.append(loop.create_task(exec_task(task, stage)))

        # Wait for all tasks to complete before signaling the next to resolve
        await asyncio.gather(*stage_coros)
        await self.queues[i + 1].put(self.sentinel)
        log.debug(
            f"""Stage {stage.func.__name__} completed in {time.time() - time_start:.2f}
            seconds."""
        )
