import pytest
import pytest_asyncio
from assertpy import assert_that
from loguru import logger
from motor.motor_asyncio import AsyncIOMotorClient

from mq import mq
from mq.utils import MongoDBConnectionParameters
from tests.jobs import downstream2


@pytest.fixture
def q():
    client = AsyncIOMotorClient("mongodb://localhost:27017")
    return client["mq"]["mq"]


@pytest_asyncio.fixture(autouse=True)
async def run_around(q):
    await q.drop()
    await mq.init(MongoDBConnectionParameters())

    worker = mq.default_worker(channel="test")
    await worker.start()
    logger.debug("worker started")

    yield

    logger.debug("setting down worker...")
    worker.worker_stop_event.set()
    worker._process_executor.terminate()


@pytest.mark.asyncio
async def test_enqueue(q):
    command = await downstream2.mq(1)
    assert_that(await q.count_documents({})).is_equal_to(1)

    assert_that(
        {
            "f": b"\x80\x05\x95&\x00\x00\x00\x00\x00\x00\x00\x8c\ntests.jobs\x94\x8c\x0bdownstream2\x94\x93\x94K\x01\x85\x94}\x94\x87\x94.",
            "payload": None,
            "computed_downstream": {},
            "locked_by": None,
            "status": "waiting",
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
