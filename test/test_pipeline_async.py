import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from time import sleep

import pytest

from beets.importer_n import AsyncPipeline

log = logging.getLogger("beets")


# Some simple pipeline stage functions for testing.
Task = int


async def _produce(num: Task = 5):
    for i in range(num):
        await asyncio.sleep(0.01)
        log.debug(f"Producing {i}")
        yield i


async def _work(num: Task):
    log.debug(f"Working {num}")
    yield num**2


async def _consume(result: Task):
    yield result


# Worker that raises an exception.
class PipelineError(Exception):
    pass


async def _exc_work(num: Task = 3):
    yield num
    raise PipelineError()


@pytest.mark.parametrize(
    "executor",
    [None, ThreadPoolExecutor()],
)
class TestSimplePipeline:
    @pytest.mark.asyncio(loop_scope="function")
    async def test_run_async(self, executor) -> None:
        pl = AsyncPipeline(executor)
        pl.set_producer(_produce)
        pl.add_stage(_work)
        pl.add_stage(_consume)
        results = await pl.collect_results()
        assert sorted(results) == [i**2 for i in range(5)]

    @pytest.mark.asyncio(loop_scope="function")
    async def test_chain_pipes(self, executor) -> None:
        pl = AsyncPipeline(executor)
        pl.set_producer(_produce)
        pl.add_stage(_work)

        pl2 = AsyncPipeline(executor)
        pl2.set_producer(pl)
        pl2.add_stage(_work)

        results = await pl2.collect_results()
        assert sorted(results) == [(i**2) ** 2 for i in range(5)]


@pytest.mark.parametrize(
    "executor",
    [None, ThreadPoolExecutor()],
)
class TestExceptionInPipeline:
    @pytest.mark.asyncio(loop_scope="function")
    async def test_in_stage(self, executor) -> None:
        pl = AsyncPipeline(executor)
        pl.set_producer(_produce)
        pl.add_stage(_exc_work)
        with pytest.raises(PipelineError):
            await pl.collect_results()

        pl = AsyncPipeline(executor)
        pl.set_producer(_produce)
        pl.add_stage(_work)
        pl.add_stage(_exc_work)
        with pytest.raises(PipelineError):
            await pl.collect_results()

    @pytest.mark.asyncio(loop_scope="function")
    async def test_in_producer(self, executor) -> None:
        pl = AsyncPipeline(executor)
        pl.set_producer(_exc_work)
        pl.add_stage(_work)
        with pytest.raises(PipelineError):
            await pl.collect_results()


def _work_sync(num: Task):
    sleep(0.01)
    yield num**2


def _produce_sync(num: Task = 5):
    for i in range(num):
        yield i


def _consume_sync(result: Task):
    yield result


@pytest.mark.parametrize(
    "executor",
    [None, ThreadPoolExecutor()],
)
class TestCompatibilityWithNoneAsync:
    @pytest.mark.asyncio(loop_scope="function")
    async def test_run_sync(self, executor) -> None:
        pl = AsyncPipeline(executor)
        pl.set_producer(_produce_sync)
        pl.add_stage(_work_sync)
        pl.add_stage(_consume_sync)
        results = await pl.collect_results()
        assert sorted(results) == [i**2 for i in range(5)]
