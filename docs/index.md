# Welcome to mq docs !

mq is a *publish/subscribe* (or queueing system) mechanism based on `mongodb`. It allows to launch small tasks
in an asynchronous way. Several tools exist but using other datastore such as *Redis* (rq, ...) so if you already use
redis, it may be a better choice.

mq is a hobby project mainly to learn *asyncio* module, and multiprocessing condition primitives.

## Getting started 🚀

```shell
pip install mq
```

or with poetry:

```shell
poetry add mq
```

## How it works ⁉

**mq** can work in several ways. Usually, we **enqueue** tasks in a process, and launch another process
to **dequeue** these tasks and perform their execution. *mq* support this in several ways:

- launch worker process in same script becoming a subprocess of the main process
- launch worker process in another script.
- launch worker in a thread for heavy IO tasks.

## Examples 🎨

```py title="minimal example" linenums="1"
import random
import asyncio
from functools import partial

from loguru import logger

from mq.mq import mq, job


@job(channel="test")
async def job_test(a, b):
    await asyncio.sleep(1)
    return a + b


async def main():
    await mq.init(
        mongo_uri="mongodb://localhost:27017",
        db_name="mq",
        collection="mq",
    )

    # start worker process in same process
    mq.worker_for("default").start()

    # enqueue job, coroutine function
    r1, r2 = random.randint(1, 100), random.randint(1, 100)
    job_result = await job_test.mq(r1, r2)

    # add a callback when it' s done !
    job_result.add_done_callback(partial(logger.debug, "Got a result {} !"))


if __name == "__main__":
    asyncio.run(main())
```

You can also simply wait for the job result

```py
await job_result.wait_for_result(timeout=None)
```

Or cancel the job while it is running:

```py
await job_result.cancel(timeout=None)
```

## Launch worker process in another script

Needs to add a parameter in the init function of mq, in the enqueuing process

```py hl_lines="16"
await mq.with_process_connection(
    # specify connection parameters to the main manager process
    MQManagerConnectionParameters(
        url= "127.0.0.1",
        port=50000,
        authkey=b"abracadabra"
    )
).init(
    # specify mongodb connection
    MongoDBConnectionParameters(
        mongo_uri="mongodb://localhost:27017",
        db_name="mq",
        collection="mq",
    ),
    # add this one to start manager server
    start_server=True,
)
```

Now in another script:

```py hl_lines="15"
await mq.with_process_connection(
    # specify connection parameters to the main manager process
    MQManagerConnectionParameters(
        url= "127.0.0.1",
        port=50000,
        authkey=b"abracadabra"
    ) #(1)
).init(
    # specify mongodb connection
    MongoDBConnectionParameters(
        mongo_uri="mongodb://localhost:27017",
        db_name="mq",
        collection="mq",
    ),
    in_worker_process=True,
)
```

1. Needs to connect to other process and acceed to shared memory. 
Process can also be remote ☁ !