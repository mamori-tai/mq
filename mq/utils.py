import asyncio
import contextlib


def _cancel_all_tasks(loop):
    try:
        tasks = asyncio.gather(*asyncio.all_tasks(loop), return_exceptions=True)
        tasks.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            loop.run_until_complete(tasks)
        loop.run_until_complete(loop.shutdown_asyncgens())
    finally:
        loop.close()
