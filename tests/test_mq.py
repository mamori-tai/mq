import asyncio
from unittest.mock import Mock

import pytest
import pytest_asyncio
from assertpy import assert_that
from loguru import logger
from motor.motor_asyncio import AsyncIOMotorClient

from mq import mq
from mq._job import JobStatus
from mq._queue import DeleteJobError, JobCommand
from mq._worker import WorkerType
from mq.utils import MongoDBConnectionParameters
from tests.jobs import downstream2, job_test

# @pytest.fixture(scope="session", autouse=True)
# def event_loop():
#    loop = asyncio.new_event_loop()
#    yield loop
#    loop.close()


@pytest.fixture
def q():
    client = AsyncIOMotorClient("mongodb://localhost:27017")
    return client["mq"]["mq"]


@pytest_asyncio.fixture(autouse=True)
async def worker(q):
    await q.drop()
    logger.debug("init MQ")
    await mq.init(MongoDBConnectionParameters())

    worker = mq.default_worker(channel="test", worker_type=WorkerType.THREAD)
    await worker.start()
    logger.debug("worker started")

    yield

    # logger.debug("setting down worker...")
    await asyncio.sleep(2)
    await worker.terminate()


@pytest.mark.asyncio
async def test_enqueue(q):

    command = await downstream2.mq(1)
    assert_that(await q.count_documents({})).is_equal_to(1)

    assert_that(
        {
            "f": b"\x80\x05\x95&\x00\x00\x00\x00\x00\x00\x00\x8c\ntests.jobs\x94\x8c\x0bdownstream2\x94\x93\x94K\x01\x85\x94}\x94\x87\x94.",
            "payload": None,
            "computed_downstream": {},
            "last_duration": None,
            "last_run_at": None,
            "result": None,
            "extra": {
                "retry": {
                    "stop": b"\x80\x05N.",
                    "retry": b"\x80\x05N.",
                    "wait": b"\x80\x05N.",
                }
            },
        }
    ).is_subset_of(await command.job())


@pytest.mark.asyncio
async def test_wait_for_result(q):

    command: JobCommand = await downstream2.mq(1)
    assert_that(await command.wait_for_result()).is_equal_to(2)


@pytest.mark.asyncio
async def test_cancel(q):
    command: JobCommand = await downstream2.mq(1)
    await command.cancel()

    assert_that((await command.job())["status"]).is_equal_to(JobStatus.CANCELLED)


@pytest.mark.asyncio
async def test_delete(q):

    command: JobCommand = await downstream2.mq(1)
    logger.debug(await command.cancel())
    await command.delete()
    assert_that(await q.count_documents({})).is_equal_to(0)

    command: JobCommand = await downstream2.mq(1)
    with pytest.raises(DeleteJobError):
        await command.delete()


@pytest.mark.asyncio
async def test_downstream(q):

    command: JobCommand = await job_test.mq(1, 2)
    assert_that(await q.count_documents({})).is_equal_to(3)
    docs = await q.find({"_id": {"$nin": [command.job_id]}}).to_list(length=100)
    status = {d["status"] for d in docs}
    assert_that(status).is_length(1)
    assert_that({JobStatus.WAITING_FOR_UPSTREAM}).is_subset_of(status)


@pytest.mark.asyncio
async def test_downstream_cancel(q):

    command: JobCommand = await job_test.mq(1, 2)
    await command.cancel()
    docs = await q.find({"_id": {"$nin": [command.job_id]}}).to_list(length=100)
    status = {d["status"] for d in docs}
    assert_that(status).is_length(1)
    assert_that({JobStatus.CANCELLED}).is_subset_of(status)


@pytest.mark.asyncio
async def test_add_done_callback(q):

    command: JobCommand = await downstream2.mq(1)
    m = Mock()

    def cb(_):
        logger.debug("called")
        m.method()

    command.add_done_callback(cb)
    await asyncio.sleep(3)
    m.method.assert_called_once()
