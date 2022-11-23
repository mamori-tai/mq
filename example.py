import asyncio
import random

from loguru import logger

from mq import mq
from mq.utils import MongoDBConnectionParameters, MQManagerConnectionParameters
from tests.common import job_test

# import tests.common as common
# cloudpickle.register_pickle_by_value(common)
# @job(channel="test", schedule=every(10).seconds)
# async def job_test(a, b):
#    await asyncio.sleep(0.1)
#    return a + b


class TaskRunner:
    def __init__(self, a, b):
        self.a = a
        self.b = b

    async def run(self, current_job, enqueue_f):
        # if current_job.f is None:
        #   logger.debug("None value... returning")
        #   return
        logger.debug(enqueue_f)
        logger.debug("HELLO")
        payload = current_job.payload
        x = payload["a"] / payload["b"]  # noqa: F841
        await asyncio.sleep(0.1)
        command = await enqueue_f(job_test, 1, 2)
        logger.debug("from func {}", await command.wait_for_result())
        # async with aiohttp.ClientSession() as session:
        #    async with session.get('http://httpbin.org/get') as resp:
        #        print(resp.status)
        #        print(await resp.text())


# @register_task_runner(channel="default")
def my_worker_function(current_job):
    logger.debug(current_job.payload)


if __name__ == "__main__":

    # multiprocessing.current_process().authkey = b"abracadabra"    # <--- HERE
    async def main():
        # async with mongodb_connection_parameters(
        #        mongo_uri="mongodb://localhost:27017",
        #        db_name="mq",
        #        collection="mq"
        # ) as mq:
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
        worker = await mq.default_worker(
            channel="default"
        ).start()  # .max_concurrency(3).every(3).seconds.start()
        logger.info("Worker started !")
        # await worker.scale_up(2)
        for i in range(1):
            r1, r2 = random.randint(1, 100), random.randint(1, 100)
            # job_result = await job_test.mq(r1, r2)
            # job_result.add_done_callback(partial(logger.debug, "Got a result {} !"))
            # await asyncio.sleep(3)

            # await job_result.cancel()
            # job_result.add_done_callback(lambda x: logger.debug('Second cb {}', x + 123))
            # logger.debug("Enqueuing...")
            job_result_2 = await job_test.mq(r1, r2)
            # logger.debug("Enqueued one job")
            # job_result = await mq.enqueue(payload=dict(a=1, b=1))

            # logger.debug("before wait for result")
            logger.debug("result {}", await job_result_2.cancel())
            # logger.debug("end waiting for result")
            # job_result.add_done_callback(lambda _: logger.debug("Coucou"))
            # await asyncio.sleep(2)
            # logger.debug("is cancel {}", await job_result.cancel())
            # res = await job_result.wait_for_result()
            # logger.debug(res)
            # logger.debug("Cancelled: {}", cancelled)

        # worker_info = mq.worker_for(channel="default").start()

        # logger.debug(await job_result_2.wait_for_result())

        # closing the worker
        # await asyncio.sleep(3)
        # logger.debug("terminating")
        # await worker.terminate()
        logger.debug(worker.future)
        return worker.future

    asyncio.run(main())
