# Dequeuing

Mq provides several dequeuing mechanisms. Could be in a subprocess, completely in another
process or finally in thread for IO heavy tasks.

## Launching a worker subprocess

## Launching a thread process

## Launching worker in another process

## Specifying a task runner for channel

```python
@register_task_runner
def print_payload(current_job):
    logger.debug(current_job.payload)
```