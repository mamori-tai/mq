import asyncio

from tenacity import stop_after_attempt

from mq import job


@job(channel="test")
async def downstream2(a):
    return a + 1


@job(channel="test", downstream=[downstream2])
async def downstream(a):
    return a + 1


@job(
    channel="test",
    downstream=[downstream]
    # schedule=every(10).seconds,
    # retry=stop_after_delay(1) | stop_after_attempt(3),
)
async def job_test(a, b):
    await asyncio.sleep(0.1)
    return a + b


@job(
    channel="test",
    # schedule=every(10).seconds,
    stop=stop_after_attempt(3),
)
async def test_retry(a, b):
    await asyncio.sleep(0.1)
    raise ValueError("")
