import asyncio
import contextlib
import dataclasses
import multiprocessing
import pickle
import time
from functools import partial
from typing import Type

import async_timeout
import dill


@dataclasses.dataclass
class MongoDBConnectionParameters:
    """ """

    mongo_uri: str
    db_name: str
    collection: str


@dataclasses.dataclass
class MQManagerConnectionParameters:
    url: str = ""
    port: int = 50000
    authkey: bytes = b"abracadabra"


def _cancel_all_tasks(loop) -> None:
    """

    Args:
        loop:

    Returns:

    """
    try:
        tasks = asyncio.gather(*asyncio.all_tasks(loop), return_exceptions=True)
        tasks.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            loop.run_until_complete(tasks)
        loop.run_until_complete(loop.shutdown_asyncgens())
    finally:
        loop.close()


async def wait_for_event_cleared(ev: multiprocessing.Event, timeout: float = 0.5):
    """
    Tried condition a lot
    Args:
        ev:
        timeout:

    Returns:

    """

    def _check_ev():
        while 1:
            event_is_not_set = not ev.is_set()
            if event_is_not_set:
                return True
            time.sleep(0.1)

    async def _wait():
        await asyncio.sleep(timeout)

    t = asyncio.create_task(asyncio.to_thread(_check_ev))
    w = asyncio.create_task(_wait())
    done, pending = await asyncio.wait({t, w}, return_when=asyncio.FIRST_COMPLETED)
    for _t in pending:
        _t.cancel()
    return t in done


@contextlib.contextmanager
def suppress(exc_type: Type[BaseException]):
    with contextlib.suppress(exc_type):
        yield


loads = dill.loads
dumps = partial(dill.dumps, protocol=pickle.HIGHEST_PROTOCOL, byref=False, recurse=True)
