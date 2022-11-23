import asyncio
import threading
import typing
from asyncio import CancelledError
from typing import Any, Callable, Coroutine

import async_timeout
import pymongo

# noinspection PyProtectedMember
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection
from pymongo.errors import CollectionInvalid

from mq._job import Job, JobStatus
from mq._scheduler import SchedulerProtocol
from mq.utils import EnqueueMixin, MongoDBConnectionParameters, loads, wait_for_event_cleared

if typing.TYPE_CHECKING:
    from mq.mq import P


class JobCommand:
    def __init__(
        self,
        job_id: str,
        q: AsyncIOMotorCollection,
        events: dict[str, list[threading.Event]],
    ):
        self._job_id = job_id
        self.q = q
        self.events = events

        self._result = None
        self._tasks = set()

    @property
    def job_id(self):
        return self._job_id

    async def job(self, as_job: bool = False) -> dict[str, Any] | Job:
        job_as_dict = await self.q.find_one({"_id": self._job_id})
        if as_job:
            return Job(**job_as_dict)
        return job_as_dict

    async def cancel(self) -> bool:
        """
        Returns:

        """
        doc = await self.q.find_one_and_update(
            {"_id": self._job_id, "status": JobStatus.WAITING},
            {"$set": {"status": JobStatus.CANCELLED}},
        )
        if doc is not None:
            return True

        ev = self.events.get(self._job_id)[1]
        if ev is None:
            raise ValueError("Could not find event")

        ev.set()
        return await wait_for_event_cleared(ev, timeout=1)

    async def wait_for_result(self, timeout: float = 10):
        event = self.events.get(self._job_id)[0]
        if event is None:
            raise ValueError("Could not find event")

        try:
            async with async_timeout.timeout(timeout):
                await asyncio.to_thread(event.wait)
        except asyncio.TimeoutError:
            return None

        refreshed_job = await self.job()

        assert refreshed_job is not None, "Job not found !"
        if (result := refreshed_job.get("result")) is None:
            return None
        self._result = loads(result)
        return self._result

    def _done_cb(self, task, cb):
        self._tasks.discard(task)
        try:
            return cb(task.result())
        except CancelledError:
            return cb(None)

    def add_done_callback(self, cb: Callable | Coroutine):
        task = asyncio.get_running_loop().create_task(self.wait_for_result())
        task.add_done_callback(lambda t: self._done_cb(t, cb))


class JobQueue(EnqueueMixin):
    def __init__(
        self,
        *,
        mongodb_connection: MongoDBConnectionParameters,
        shared_memory: "P" = None,
        scheduler: SchedulerProtocol,
    ):
        self._mongodb_connection = mongodb_connection
        self._client = AsyncIOMotorClient(mongodb_connection.mongo_uri)
        self.db = self._client[mongodb_connection.db_name]
        self.q: AsyncIOMotorCollection = None
        self._shared_memory = shared_memory
        self.scheduler = scheduler

    async def init(self):
        if not await self._exists():
            await self._create()
        self.q = self.db[self.connection_parameters.collection]

    @property
    def connection_parameters(self):
        return self._mongodb_connection

    async def _create(self):
        collection = self.connection_parameters.collection
        try:
            await self.db.create_collection(collection)
            await self.db[collection].create_index([("status", pymongo.ASCENDING)])
        except CollectionInvalid as e:
            raise ValueError(f"Collection {collection=} already created") from e

    async def _exists(self):
        return (
            self.connection_parameters.collection
            in await self.db.list_collection_names()
        )

    async def enqueue(
        self, f: Callable[..., Any] | Coroutine | None, *args: Any, **kwargs: Any
    ) -> JobCommand:
        job = await self.enqueue_job(f, *args, **kwargs)

        # set up different events
        events = self._shared_memory.events()
        manager = self._shared_memory.manager
        # noinspection PyProtectedMember
        events[job.id] = manager.list([manager.Event(), manager.Event()])

        # returning the job command
        # noinspection PyProtectedMember
        return JobCommand(job_id=job._id, q=self.q, events=self._shared_memory.events())
