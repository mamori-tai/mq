import asyncio

from loguru import logger

from mq import mq
from mq.utils import MongoDBConnectionParameters, MQManagerConnectionParameters
from tests.common import test_retry

if __name__ == "__main__":

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
        ).start()
        logger.info("Worker started !")

        #for i in range(10):
        #    r1, r2 = random.randint(1, 100), random.randint(1, 100)
        #    job_result_2 = await job_test.mq(r1, r2)

        #    first_leave = (await job_result_2.leaves())[0]
        #    r = random.randint(1, 100)
        #    command_leave: JobCommand = job_result_2.command_for(first_leave)
        #    logger.debug("{}", r)
        #    if r > 50:
        #        logger.debug("canceling...")
        #        await command_leave.cancel()

        await test_retry.mq(1,2)

            #logger.debug("leaves {}", await command_leave.wait_for_result())
            #logger.debug("delete {}", await command_leave.delete())

        #await worker.terminate()

    asyncio.run(main())
