# Enqueueing

You can customize the way job are enqueued and executed. You can pass several interesting arguments
such as:

- schedule
- retry
- stop
- wait
- downstream


## Scheduling jobs ⏱

For scheduling, we use the amazing [schedule](https://schedule.readthedocs.io/en/stable/) library.
When enqueuing a job, you can specify the {==schedule==} keyword.

```python
@job(channel="main", schedule=every(10).seconds)
async def add_job(a: int, b: int) -> int:
    await asyncio.sleep(1)
    return a + b
```


## Retrying policy ◀

The keyword *retry, stop* and *wait* are handled by [tenacity](https://tenacity.readthedocs.io/en/latest/) library.

```python
@job(channel="main", stop=stop_after_attempt(3))
async def add_job(a: int, b: int) -> int:
    await asyncio.sleep(1)
    return a + b
```

To see {==tenacity==} in action, do not hesitate to visit their documentation !

## Downstream ♻

Very often you want to define relation between tasks (DAG or *directed acyclic graph*). For this use case, you
can use the {==downstream==} keyword argument

```py
import asyncio
from mq import job

@job(channel="main")
async def add_100(a: int) -> int:
    return a + 100


@job(channel="main", downstream=[add_100])
async def add_job(a: int, b: int) -> int:
    await asyncio.sleep(1)
    return a + b
```

!!! note
    A task cancellation leads to cancellation of all *downstream* tasks

### Getting child jobs result

To get result of a downstream job, you can do the following ({==leaves==} correspond to job id of the last task of
the flow):

```py
async def main():
    # enqueue job...
    command = await add_job.mq(1, 2)
    
    # get the child job
    child_job_id = await command.leaves()[0]
    child_command = command.command_for(child_job_id)
    result = await child_command.wait_for_result()
```

