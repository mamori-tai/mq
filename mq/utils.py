import abc
import asyncio
import dataclasses
import multiprocessing
import pickle
import typing
import uuid
from functools import partial
from typing import Any, Callable, Coroutine, Protocol

import async_timeout
import dill
from motor.motor_asyncio import AsyncIOMotorCollection

from mq._job import Job

if typing.TYPE_CHECKING:
    from mq._scheduler import SchedulerProtocol


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


async def wait_for_event_cleared(ev: multiprocessing.Event, timeout: float = 0.5):
    """
    Tried condition a lot
    Args:
        ev:
        timeout:

    Returns:

    """

    async def _check_ev():
        while 1:
            event_is_not_set = not ev.is_set()
            if event_is_not_set:
                return True
            await asyncio.sleep(0.1)

    try:
        async with async_timeout.timeout(timeout):
            await _check_ev()
        return True
    except asyncio.TimeoutError:
        return False


loads = dill.loads
dumps = partial(dill.dumps, protocol=pickle.HIGHEST_PROTOCOL, byref=False, recurse=True)


class EnqueueProtocol(Protocol):
    q: AsyncIOMotorCollection
    scheduler: "SchedulerProtocol"

    async def enqueue(self):
        ...


class EnqueueMixin(abc.ABC):
    async def enqueue_job(
        self: EnqueueProtocol,
        f: Callable[..., Any] | Coroutine | None,
        *args: Any,
        **kwargs: Any,
    ) -> Job:
        assert self.q is not None
        job_id = str(uuid.uuid4())
        payload = kwargs.pop("payload", None)

        # get the stuff that is passed to the schedules kwarg
        schedule_obj = kwargs.pop("schedule", None)
        job = Job(
            _id=job_id,
            f=dumps((f, args, kwargs)) if f is not None else None,
            payload=payload,
        )

        self.scheduler.on_enqueue_job(job, schedule_obj)
        await self.q.insert_one(dataclasses.asdict(job))

        return job
