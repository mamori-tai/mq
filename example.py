import random
import asyncio
from functools import partial

from loguru import logger

from mq._queue import InsertJobResult
from mq.mq import mq, job


@job(channel="test")
async def job_test(a, b):
    await asyncio.sleep(12)
    return a + b


if __name__ == "__main__":

    async def main():
        await mq.init(
            mongo_uri="mongodb://localhost:27017",
            db_name="mq",
            collection="mq",
        )
        mq.worker_for("default").start()
        r1, r2 = random.randint(1, 100), random.randint(1, 100)
        job_result: InsertJobResult = await job_test.mq(r1, r2)
        job_result.add_done_callback(partial(logger.debug, "Got a result {} !"))
        await asyncio.sleep(3)

        await job_result.cancel()
        # job_result.add_done_callback(lambda x: logger.debug('Second cb {}', x + 123))

        while 1:
            await asyncio.sleep(1)

    asyncio.run(main())
