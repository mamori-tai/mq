import asyncio
import random

from loguru import logger

from mq import mq

# noinspection PyProtectedMember
from mq._queue import JobCancelledError
from mq.utils import MongoDBConnectionParameters, MQManagerConnectionParameters

from .jobs import test_retry


async def main():
    await mq.with_process_connection(MQManagerConnectionParameters()).init(
        MongoDBConnectionParameters(
            mongo_uri="mongodb://localhost:27017",
            db_name="mq",
            collection="mq",
        ),
        start_server=False,
    )
    await mq.job_queue.db.mq.drop()
    await mq.job_queue.db.mq_workers.drop()
    assert mq.initialized is True
    logger.info("starting worker...")
    await mq.default_worker(channel="default").start()
    logger.info("Worker started !")

    for i in range(1):
        r1, r2 = random.randint(1, 100), random.randint(1, 100)
        job_result_2 = await test_retry.mq(r1, r2)

        try:
            logger.debug(await job_result_2.wait_for_result())
        except JobCancelledError:
            logger.error("task cancelled")


if __name__ == "__main__":
    asyncio.run(main())
