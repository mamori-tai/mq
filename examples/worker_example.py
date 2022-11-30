import asyncio

from loguru import logger

from mq import mq, register_task_runner
from mq.utils import MongoDBConnectionParameters, MQManagerConnectionParameters


@register_task_runner(channel="test")
def echo_job(current_job):
    logger.debug(current_job.payload)


async def main():
    mongodb_params = MongoDBConnectionParameters(
        mongo_uri="mongodb://localhost:27017", db_name="mq", collection="mq"
    )
    await mq.with_process_connection(MQManagerConnectionParameters()).init(
        mongodb_params, in_worker_process=True
    )
    mq.worker_with_runner(echo_job).start()


if __name__ == "__main__":
    asyncio.run(main())
