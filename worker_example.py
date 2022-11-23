import asyncio

from loguru import logger

from mq import mq, register_task_runner
from mq.utils import MongoDBConnectionParameters, MQManagerConnectionParameters


@register_task_runner()
def print_payload(current_job):
    logger.debug(current_job.payload)


if __name__ == "__main__":

    async def main():
        mongodb_params = MongoDBConnectionParameters(
            mongo_uri="mongodb://localhost:27017", db_name="mq", collection="mq"
        )
        await mq.with_process_connection(MQManagerConnectionParameters()).init(
            mongodb_params,
        )
        worker = mq.distant_worker(channel="*").with_task_runner(print_payload).start()
        await asyncio.sleep(3)
        await worker.terminate()

    asyncio.run(main())
    # worker = Worker(connection_parameters=mongodb_params, channel="default", connect_to_server=True)
    # worker.connect()
    # worker.start()
