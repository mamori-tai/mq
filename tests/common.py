import asyncio

from mq import job


@job(channel="test")
async def job_test(a, b):
    await asyncio.sleep(0.1)
    return a + b
