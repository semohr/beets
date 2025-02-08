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
import logging
import time
from abc import ABC
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncIterable,
    Callable,
    Generic,
    Optional,
)

from .pipeline_stages import (
    Args,
    MutatorStage,
    Producer,
    Return,
    Returns,
    Sentinel,
    Stage,
    StageFunc,
    StageI,
    Task,
)

if TYPE_CHECKING:
    from concurrent.futures import Executor, ThreadPoolExecutor

    from typing_extensions import Unpack


log = logging.getLogger("beets")


class Pipeline(ABC, Generic[Task, Return]):
    """A pipeline of stages that process tasks."""

    producer: Producer[Task, Return, Unpack[tuple]]
    stages: list[StageI[Task]]

    def __init__(self):
        self.stages = []

    def set_producer(
        self,
        func: Callable[[Unpack[Args]], Return] | Return,
        *args: Unpack[Args],
    ) -> None:
        """Set the producer of the pipeline."""
        self.producer = Producer(func, args)

    def add_stage(
        self,
        func: StageFunc[Unpack[Args], Task, Return],
        *args: Unpack[Args],
    ) -> None:
        """Add a stage to the pipeline.

        Converts given function to a stage and adds it to the pipeline.
        """
        self.stages.append(Stage(func, args))

    def add_mutator(
        self,
        func: Callable[[Unpack[Args], Task], Any],
        *args: Unpack[Args],
    ) -> None:
        """Add a mutator to the pipeline.

        Converts given function to a mutator stage and adds it to the pipeline.
        """
        self.stages.append(MutatorStage(func, args))

    def add_stages(
        self,
        *stages: Stage | MutatorStage,
    ):
        """Add multiple stages to the pipeline."""
        for s in stages:
            self.stages.append(s)

    @property
    def consumer(
        self,
    ) -> StageI[Task]:
        return self.stages[-1]


class AsyncPipeline(Pipeline[Task, Returns[Task]]):
    """An asynchronous pipeline of stages that process tasks.

    Using an asynchronous pipeline allows for the concurrent processing of tasks
    without blocking the execution of other tasks. This is particularly beneficial
    when dealing with I/O-bound operations, as it permits other tasks to
    progress while waiting for external resources.

    Possible todos: At the moment this just runs forever if any producer
    or stage doesn't yield a result. We could add a timeout to make it
    more resilient.
    """

    # Pass tasks between stages using queues
    queues: list[asyncio.Queue[Task | Sentinel]]
    # Sentinel value to indicate the end of the pipeline
    sentinel = Sentinel()
    # Executor for running tasks
    executor: Optional[Executor]

    def __init__(self, executor: Optional[ThreadPoolExecutor] = None):
        """Create a new pipeline.

        Parameters
        ----------
        executor : Optional[ThreadPoolExecutor], optional
            The executor to run blocking tasks, by default None
            If None, blocking tasks are run in the main thread!

        To simulate the old parallel pipeline behavior, you can pass
        executor=ThreadPoolExecutor().

        Possible todos: In theory this code is relatively easy to adapt to
        use a process pool executor. This would truly allow for parallel
        processing of tasks. But this would need another queue class and will
        most likely not yield much performance improvements as the tasks are
        mostly i/o bound.
        """
        super().__init__()
        self.executor = executor

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
        potential_exceptions = await asyncio.gather(
            *coros, return_exceptions=True
        )
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

    async def _run_stage(
        self,
        i: int,
        stage: StageI[Task],
    ):
        """Create an asyncio coroutines for each task.

        Enqueues the results for the next stage. Stops when the sentinel is
        reached.
        """
        time_start = time.time()
        loop = asyncio.get_running_loop()

        async def exec_task(
            task: Task,
            stage: StageI[Task],
        ):
            # Run task in executor
            try:
                await stage.collect_in_queue(
                    task, self.queues[i + 1], self.executor
                )
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
            f"Stage completed in {time.time() - time_start:.2f}s: {stage.func.__name__}"
        )
