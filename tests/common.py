import asyncio

from tenacity import stop_after_delay, stop_after_attempt

from mq import job, every


@job(
    channel="test",
    schedule=every(10).seconds,
    retry=stop_after_delay(1) | stop_after_attempt(3),
)
async def job_test(a, b):
    await asyncio.sleep(0.1)
    return a + b
