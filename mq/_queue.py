import asyncio
import pickle
import typing
import uuid
from asyncio import CancelledError
from typing import Callable, Any, Coroutine

import pymongo
from loguru import logger
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection
from pymongo.errors import CollectionInvalid

from mq._job import Job
from mq.utils import MongoDBConnectionParameters, wait_for_event_cleared, dumps

if typing.TYPE_CHECKING:
    from mq.mq import P


class JobCommand:
    def __init__(self, job_id: str, job_queue: 'JobQueue', shared_memory: 'P'):
        self._job_id = job_id
        self._job_queue = job_queue
        self._shared_memory = shared_memory
        self._init_events()

        self._result = None
        self._tasks = set()

    def _init_events(self):
        self._shared_memory.init_events_for_job_id(self._job_id)

    @property
    def job_id(self):
        return self._job_id

    async def job(self):
        return await self._job_queue._q.find_one({"_id": self._job_id})

    async def cancel(self) -> bool:
        """
        Returns:

        """
        ev = self._shared_memory.cancel_event_by_job_id.get(self._job_id)
        if ev is None:
            raise ValueError("Could not find event")

        ev.set()
        return await wait_for_event_cleared(ev)

    async def wait_for_result(self):
        event = self._shared_memory.result_event_by_job_id.get(self._job_id)
        if event is None:
            raise ValueError("Could not find event")

        await asyncio.to_thread(event.wait)

        refreshed_job = await self.job()

        assert refreshed_job is not None, "Job not found !"
        if (result := refreshed_job.get("result")) is None:
            return None
        self._result = pickle.loads(result)
        return self._result

    def _done_cb(self, task, cb):
        self._tasks.discard(task)
        try:
            return cb(task.result())
        except CancelledError:
            return cb(None)

    def add_done_callback(self, cb: Callable | Coroutine):
        task = asyncio.get_event_loop().create_task(self.wait_for_result())
        task.add_done_callback(lambda t: self._done_cb(t, cb))


class JobQueue:
    def __init__(
        self,
        *,
        mongodb_connection: MongoDBConnectionParameters,
        shared_memory: "P" = None,
    ):
        self._mongodb_connection = mongodb_connection
        self._client = AsyncIOMotorClient(mongodb_connection.mongo_uri)
        self._db = self._client[mongodb_connection.db_name]
        self._q: AsyncIOMotorCollection = None
        self._shared_memory = shared_memory

    async def init(self):
        if not await self._exists():
            await self._create()
        self._q = self._db[self.connection_parameters.collection]

    @property
    def connection_parameters(self):
        return self._mongodb_connection

    async def _create(self):
        collection = self.connection_parameters.collection
        try:
            logger.info(f"creating queue on {collection=}")
            await self._db.create_collection(collection)
            logger.info(f"creating index on queue {collection=} on field 'status'")
            await self._db[collection].create_index([("status", pymongo.ASCENDING)])
        except CollectionInvalid as e:
            raise ValueError(f"Collection {collection=} already created") from e

    async def _exists(self):
        return (
            self.connection_parameters.collection
            in await self._db.list_collection_names()
        )

    async def enqueue(
        self, f: Callable[..., Any] | Coroutine | None, *args: Any, **kwargs: Any
    ) -> JobCommand:
        assert self._q is not None
        job_id = str(uuid.uuid4())
        payload = kwargs.pop("payload", None)
        data = dict(f=dumps((f, args, kwargs))) if f is not None else dict(payload=payload)
        logger.debug(data)
        job = Job(id=job_id, **data)
        job_command = JobCommand(
            job_id=job_id, job_queue=self, shared_memory=self._shared_memory
        )
        await self._q.insert_one(job.dict(by_alias=True))
        return job_command

