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

import asyncio
from abc import ABC
from typing import (
    Any,
    AsyncGenerator,
    AsyncIterable,
    Awaitable,
    Callable,
    Concatenate,
    Generator,
    Generic,
    Iterable,
    Mapping,
    NamedTuple,
    Optional,
    ParamSpec,
    TypeVar,
    Union,
)

# ---------------------------------- Stages ---------------------------------- #
# Typing
P = ParamSpec("P")
R = TypeVar("R", bound=Any)
Task = TypeVar("Task")
Returns = Union[
    Iterable[Task],
    Generator[Task],
    AsyncIterable[Task],
    AsyncGenerator[Task, None],
]
StageFunc = Callable[Concatenate[Task, P], Returns[Task]]
ProducerFunc = Callable[P, Returns[Task]]
ConsumerFunc = Callable[Concatenate[Task, P], R | Awaitable[R]]


class Stage(NamedTuple, Generic[Task, P]):
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

    func: StageFunc[Task, P]
    args: tuple  # P.args: tuple (not possible to type hint)
    kwargs: Mapping[str, Any]  # P.kwargs: dict (not possible to type hint)


class Producer(NamedTuple, Generic[Task, P]):
    """A producer generates tasks for the pipeline.

    We also allow to pass an iterable of tasks as a producer.
    args and kwargs are none if the producer is a generator and
    not a callable.
    """

    func: Callable[P, Returns] | Returns
    args: Optional[tuple]  # P.args: tuple (not possible to type hint)
    kwargs: Optional[Mapping[str, Any]]  # P.kwargs: dict (not possible to type hint)


class Consumer(NamedTuple, Generic[Task, P, R]):
    """A consumer consumes tasks from the pipeline."""

    func: ConsumerFunc[Task, P, R]
    args: tuple  # P.args: tuple (not possible to type hint)
    kwargs: Mapping[str, Any]  # P.kwargs: dict (not possible to type hint)


# --------------------------------- Pipelines -------------------------------- #
class Pipeline(ABC, Generic[Task, R]):
    """A pipeline of stages that process tasks."""

    producer: Producer[Task, ...]
    stages: list[Stage[Task, ...]]
    consumer: Consumer[Task, ..., R]

    def add_stage(
        self, func: StageFunc[Task, P], *args: P.args, **kwargs: P.kwargs
    ) -> None:
        """Add a stage to the pipeline."""
        self.stages.append(Stage(func, args, kwargs))

    def set_producer(
        self, func: ProducerFunc[P, Task], *args: P.args, **kwargs: P.kwargs
    ) -> None:
        """Set the producer of the pipeline."""
        self.producer = Producer(func, args, kwargs)

    def set_consumer(
        self, func: ConsumerFunc[Task, P, R], *args: P.args, **kwargs: P.kwargs
    ):
        """Set the consumer of the pipeline."""
        self.consumer = Consumer(func, args, kwargs)


class Sentinel:
    __slots__ = ()


class AsyncPipeline(Pipeline[Task, R]):
    """An asynchronous pipeline of stages that process tasks.

    Using an asynchronous pipeline allows for the concurrent processing of tasks
    without blocking the execution of other tasks. This is particularly beneficial
    when dealing with I/O-bound operations, as it permits other tasks to
    progress while waiting for external resources.
    """

    # Pass tasks between stages using queues
    queues: list[asyncio.Queue[Task | Sentinel]]
    # Sentinel value to indicate the end of the pipeline
    sentinel = Sentinel()

    async def run_async(self):
        """Run the pipeline asynchronously."""
        self.queues = [asyncio.Queue() for _ in range(len(self.stages) + 1)]
        res = await asyncio.gather(
            self._run_producer(),
            self._run_stages(),
            self._consumer(),
        )
        return res[-1]

    async def _run_producer(self):
        """Produce tasks for the pipeline and enqueue them."""

        # Parse possible producer types
        if isinstance(self.producer.func, Callable):
            producer = self.producer.func(
                *self.producer.args, **self.producer.kwargs or {}
            )
        else:
            producer = self.producer.func

        # Enqueue tasks
        if isinstance(producer, (Iterable, Generator)):
            for task in producer:
                await self.queues[0].put(task)
        elif isinstance(producer, (AsyncIterable, AsyncGenerator)):
            async for task in producer:
                await self.queues[0].put(task)
        else:
            assert False, "Producer must be an iterable, generator, or async generator."

        # Signal the end of the pipeline
        await self.queues[0].put(self.sentinel)

    async def _run_stages(self):
        """Process tasks in the pipeline stages.

        We create a task for each (stage, task) pair and pass the results
        to the next stage on completion. This allows for concurrent processing
        of tasks in the pipeline.
        """
        coros = []
        for i, stage in enumerate(self.stages):
            coros.append(self._run_stage(i, stage))
        await asyncio.gather(*coros)

    async def _run_stage(self, i: int, stage: Stage[Task, P]):
        """
        Creates asyncio coroutines for each task in the current queue and
        enqueues the results for the next stage. Stops when the sentinel is
        reached.
        """

        async def exec_task(task: Task, stage: Stage[Task, P]):
            next_tasks = stage.func(task, *stage.args, **stage.kwargs)
            if isinstance(next_tasks, (AsyncIterable, AsyncGenerator)):
                async for new_task in next_tasks:
                    await self.queues[i + 1].put(new_task)
            else:
                for new_task in next_tasks:
                    await self.queues[i + 1].put(new_task)

        stage_coros = []
        while True:  # TODO: maybe add a timeout here
            task = await self.queues[i].get()
            if isinstance(task, Sentinel):
                break
            stage_coros.append(asyncio.create_task(exec_task(task, stage)))

        # Wait for all tasks to complete before signaling the next to resolve
        await asyncio.gather(*stage_coros)
        await self.queues[i + 1].put(self.sentinel)

    async def _consumer(self):
        """Consume the final tasks in the pipeline."""

        async def exec_consume(task: Task, consumer: Consumer[Task, P, R]):
            results = consumer.func(task, *consumer.args, **consumer.kwargs)
            if isinstance(results, Awaitable):
                return await results
            else:
                return results

        consume_coros: list[asyncio.Task[R]] = []
        while True:
            task = await self.queues[-1].get()
            if isinstance(task, Sentinel):
                break
            task = asyncio.create_task(exec_consume(task, self.consumer))
            consume_coros.append(task)

        # Wait for all tasks to complete before returning the final result
        return await asyncio.gather(*consume_coros)
