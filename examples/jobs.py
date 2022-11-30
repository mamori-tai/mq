import asyncio
import random

from tenacity import stop_after_attempt

from mq import job


@job(channel="test")
async def downstream2(a):
    return a + 1


@job(channel="test", downstream=[downstream2])
async def downstream(a):
    return a + 1


@job(channel="test", downstream=[downstream])
async def job_test(a, b):
    await asyncio.sleep(0.1)
    return a + b


@job(
    channel="test",
    stop=stop_after_attempt(3),
)
async def test_retry(a, b):
    await asyncio.sleep(0.1)
    if random.randint() > 50:
        raise ValueError("")
    return a + b


@job(channel="test")
async def test_exception():
    raise ValueError("Direct Exception")
