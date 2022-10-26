import multiprocessing
import random
import asyncio
from functools import partial

import aiohttp
from loguru import logger

from mq._runner import Runner
from mq._worker import Worker
#from mq import mq, job
from mq.utils import MongoDBConnectionParameters, MQManagerConnectionParameters
from mq import job, every, mongodb_connection_parameters, mq

#import tests.common as common
#cloudpickle.register_pickle_by_value(common)

#@job(channel="test", schedule=every(10).seconds)
#async def job_test(a, b):
#    await asyncio.sleep(0.1)
#    return a + b

from tests.common import job_test

class TaskRunner:
    def __init__(self, a, b):
        self.a = a
        self.b = b

    async def run(self, current_job):
        if current_job.f is None:
            logger.debug("None value... returning")
            return
        logger.debug(current_job.payload)
        await asyncio.sleep(0.5)
        #async with aiohttp.ClientSession() as session:
        #    async with session.get('http://httpbin.org/get') as resp:
        #        print(resp.status)
        #        print(await resp.text())


if __name__ == "__main__":


    #multiprocessing.current_process().authkey = b"abracadabra"    # <--- HERE

    async def main():
        # async with mongodb_connection_parameters(
        #        mongo_uri="mongodb://localhost:27017",
        #        db_name="mq",
        #        collection="mq"
        # ) as mq:
        await mq.with_process_connection(MQManagerConnectionParameters()).init(
            MongoDBConnectionParameters(
                mongo_uri="mongodb://localhost:27017", db_name="mq", collection="mq",
            ),
            #start_server=True
        )
        await mq.job_queue._db.mq.drop()
        assert mq.initialized is True
        worker = mq.worker_for(
            channel="default"
        ).with_task_runner(TaskRunner(123, 123)).start()  # .max_concurrency(3).every(3).seconds.start()
        #await worker.scale_up(2)
        for i in range(1):
            r1, r2 = random.randint(1, 100), random.randint(1, 100)
            # job_result = await job_test.mq(r1, r2)
            # job_result.add_done_callback(partial(logger.debug, "Got a result {} !"))
            # await asyncio.sleep(3)

            # await job_result.cancel()
            # job_result.add_done_callback(lambda x: logger.debug('Second cb {}', x + 123))

            job_result_2 = await job_test.mq(r1, r2)
            await mq.enqueue(payload=dict(a=1, b=2))

            #await asyncio.sleep(2)

            #cancelled = await job_result_2.cancel()
            #logger.debug("Cancelled: {}", cancelled)

        # worker_info = mq.worker_for(channel="default").start()

        # logger.debug(await job_result_2.wait_for_result())

        # closing the worker
        await asyncio.sleep(3)
        await worker.terminate()

    asyncio.run(main())
