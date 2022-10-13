import asyncio
import pickle
import uuid
from typing import Callable, Any, Coroutine

import pymongo
from loguru import logger
from motor.motor_asyncio import AsyncIOMotorClient

from mq._job import Job


class InsertJobResult:
    def __init__(self, job_id, job_queue, manager):
        self._job_id = job_id
        self._job_queue = job_queue
        self._manager = manager
        self._manager.result_event_by_job_id[
            self._job_id
        ] = self._manager._manager.Event()
        self._manager.cancel_event_by_job_id[
            self._job_id
        ] = self._manager._manager.Event()

        self._result = None
        self._tasks = set()

    @property
    def job_id(self):
        return self._job_id

    async def cancel(self):
        event = self._manager.cancel_event_by_job_id.get(self._job_id)
        if event is None:
            raise ValueError("Could not find event")
        event.set()
        await asyncio.to_thread(event.set)
        logger.debug("Job {} cancelled !")

    async def wait_for_result(self):
        event = self._manager.result_event_by_job_id.get(self._job_id)
        if event is None:
            raise ValueError("Could not find event")

        await asyncio.to_thread(event.wait)

        refreshed_job = await self._job_queue.q.find_one({"_id": self._job_id})

        assert refreshed_job is not None, "Job not found !"
        if (result := refreshed_job.get("result")) is None:
            return None
        self._result = pickle.loads(result)
        return self._result

    def _done_cb(self, task, cb):
        self._tasks.discard(task)
        return cb(task.result())

    def add_done_callback(self, cb: Callable | Coroutine):
        task = asyncio.get_event_loop().create_task(self.wait_for_result())
        task.add_done_callback(lambda t: self._done_cb(t, cb))


class JobQueue:
    def __init__(
        self,
        *,
        mongo_uri: str,
        db_name: str = "admin",
        queue_name: str = "job_queue",
        manager=None,
    ):
        self._mongo_uri = mongo_uri
        self._db_name = db_name
        self._db = AsyncIOMotorClient(mongo_uri)[db_name]
        self._queue_name = queue_name
        self.q = None
        self._manager = manager

    async def _init(self):
        if not await self._exists():
            await self._create()
        self.q = self._db[self._queue_name]

    async def _create(self):
        try:
            logger.info(f"creating queue collection {self._queue_name=}")
            await self._db.create_collection(self._queue_name)
            logger.info(
                f"creating index on queue collection {self._queue_name=} on field 'status'"
            )
            await self._db[self._queue_name].create_index(
                [("status", pymongo.ASCENDING)]
            )
        except Exception as e:  # pymongo.errors.CollectionInvalid as e:
            raise ValueError(f"Collection {self._queue_name=} already created") from e

    async def _exists(self):
        return self._queue_name in await self._db.list_collection_names()

    async def enqueue(
        self, f: Callable[..., Any] | Coroutine, *args: Any, **kwargs: Any
    ) -> InsertJobResult:
        assert self.q is not None
        job_id = str(uuid.uuid4())
        job = Job(id=job_id, f=pickle.dumps((f, args, kwargs)))
        await self.q.insert_one(job.dict(by_alias=True))
        return InsertJobResult(job_id=job_id, job_queue=self, manager=self._manager)

    async def update_status(self, job_id, status):
        await self.q.find_one_and_update({"_id": job_id}, status)
