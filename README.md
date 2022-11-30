# mq
[![test](https://github.com/mamori-tai/mq/actions/workflows/python-test.yml/badge.svg)](https://github.com/mamori-tai/mq/actions/workflows/python-test.yml)

*Simplest async **job queue** using mongo db in python*

## Minimal example

```python
import random
import asyncio
from functools import partial

from loguru import logger

from mq._queue import JobCommand
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

        # start worker process
        mq.worker_for("default").start()

        # enqueue job, coroutine function
        r1, r2 = random.randint(1, 100), random.randint(1, 100)
        job_result = await job_test.mq(r1, r2)

        # add a callback when its done !
        job_result.add_done_callback(partial(logger.debug, "Got a result {} !"))

        await asyncio.sleep(3)

        # simply cancel the job
        await job_result.cancel()

```

## How it works

A worker runs in its own process, spawning asyncio task (so in background) in an event loop. Synchronous functions run in the default thread pool executor.

## Ideas

- Full async
- Atomic db operations
- scheduler
- retry
