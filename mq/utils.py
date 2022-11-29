import abc
import asyncio
import concurrent.futures
import dataclasses
import multiprocessing
import pickle
import threading
import time
import typing
import uuid
from functools import partial
from multiprocessing.managers import SyncManager
from typing import Any, Callable, Coroutine, Protocol

import async_timeout
import dill
from motor.motor_asyncio import AsyncIOMotorCollection

from mq._job import Job, JobStatus

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


loads = dill.loads
dumps = partial(dill.dumps, protocol=pickle.HIGHEST_PROTOCOL, byref=False, recurse=True)


class QueueAwareProtocol(Protocol):
    q: AsyncIOMotorCollection


class EventsAwareProtocol(Protocol):
    events: dict[str, list[threading.Event]]


class EnqueueProtocol(QueueAwareProtocol, Protocol):
    scheduler: "SchedulerProtocol"

    async def enqueue_job(self):
        ...

    async def enqueue_downstream(self):
        ...


Function = Callable[..., Any] | Coroutine | None
FunctionList = list[Callable[..., Any] | Coroutine | None] | None


class EnqueueMixin(abc.ABC):
    """

    """
    async def enqueue_downstream(
        self,
        downstream: FunctionList = None,
        job_ids: dict[str, Any] = None,
        events: dict[str, list[threading.Event]] = None,
        manager: SyncManager = None,
    ):
        # assign id to each recursively
        for f in downstream or []:
            f_id = str(uuid.uuid4())
            job_ids[f_id] = {}
            await self.enqueue_job(
                job_id=f_id, status=JobStatus.WAITING_FOR_UPSTREAM, downstream_job=job_ids[f_id], events=events, manager=manager, f=(f, (), {})
            )
        return job_ids

    async def enqueue_job(
        self: EnqueueProtocol,
        job_id: str | None,
        status: JobStatus = JobStatus.WAITING,
        downstream_job: dict[str, Any] | None = None,
        events: dict[str, list[threading.Event]] | None = None,
        manager: SyncManager | None = None,
        f: tuple[Callable[..., Any] | Coroutine | None, tuple, dict] = None,
    ) -> Job:
        assert self.q is not None
        job_id = job_id or str(uuid.uuid4())
        fn, args, kwargs = f
        payload = kwargs.pop("payload", None)

        # get the stuff that is passed to the schedules kwarg
        downstream = getattr(fn, "_downstream", None)

        computed_downstream = await self.enqueue_downstream(
            downstream, downstream_job, events, manager
        )

        job = Job(
            _id=job_id,
            f=dumps((fn, args, kwargs)) if f is not None else None,
            status=status,
            computed_downstream=computed_downstream,
            payload=payload,
        )
        # setting events
        events[job.id] = manager.list([manager.Event(), manager.Event()])

        schedule_policy = getattr(fn, "_schedule", None)
        retry_policy = {
            "stop": getattr(fn, "_stop", None),
            "retry": getattr(fn, "_retry", None),
            "wait": getattr(fn, "_wait", None)
        }
        self.scheduler.on_enqueue_job(job, schedule_policy, retry_policy)
        await self.q.insert_one(dataclasses.asdict(job))

        return job


class CancelDownstreamJobMixin(QueueAwareProtocol, EventsAwareProtocol, Protocol):
    async def cancel_downstream(self, computed_downstream: dict[str, typing.Any]):
        "should be a mixin to be provided by job command"
        for job_id, child in computed_downstream.items():
            self.q.find_one_and_update(
                {"_id": job_id}, {"$set": {"status": JobStatus.CANCELLED}}
            )
            try:
                ev = self.events.get(job_id)[1]
                ev.set()
            except IndexError:
                pass
            await self.cancel_downstream(child)