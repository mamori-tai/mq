# Enqueueing options

You can customize the way job are enqueued and executed. You can pass several interesting arguments
such as:

- schedule
- retry
- stop
- wait
- downstream

For scheduling, we use the amazing [schedule](https://schedule.readthedocs.io/en/stable/) library.
The keyword *retry, stop* and *wait* are handled by [tenacity](https://tenacity.readthedocs.io/en/latest/) library.

```py
import asyncio
from tenacity import stop_after_attempt
from mq import job, every

@job(channel="main")
async def add_100(a: int) -> int:
    return a + 100


@job(channel="main", schedule=every(10).seconds, stop=stop_after_attempt(3), downstream=[add_100])
async def add_job(a: int, b: int) -> int:
    await asyncio.sleep(1)
    return a + b
```

## Getting child jobs result

Finally, you can specify downstream jobs. To get results, you can do the following:

```py
async def main():
    # enqueue job...
    command = await add_job.mq(1, 2)
    
    # get the child job
    child_job_id = await command.leaves()[0]
    child_command = command.command_for(child_job_id)
    result = await child_command.wait_for_result()
```

