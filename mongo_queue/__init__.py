import asyncio
import concurrent.futures
import contextlib
import datetime
import multiprocessing.pool
import os
import pickle
from enum import Enum
from multiprocessing import Process
from typing import Any, Callable, Coroutine, Dict, Optional

import bson
from bson import ObjectId
from loguru import logger

# noinspection PyProtectedMember
from motor.motor_asyncio import (
    AsyncIOMotorCollection,
    AsyncIOMotorCursor,
    AsyncIOMotorClient,
)
from pydantic import BaseModel, Field
from pymongo import ASCENDING, MongoClient
from pymongo.errors import CollectionInvalid

from fhir_helpers.mixins.mixin_base import with_field


class JobStatus(str, Enum):
    WAITING = "waiting"
    PENDING = "pending"
    ON_ERROR = "on_error"
    FINISHED = "finished"


@with_field
class Job(BaseModel):
    class Config:
        allow_population_by_field_name = True
        arbitrary_types_allowed = True

    id: str = Field("", alias="_id")
    f: bson.Binary
    status: JobStatus = JobStatus.WAITING
    enqueued_at: datetime.datetime = datetime.datetime.now()
    started_at: Optional[datetime.datetime] = None
    ended_at: Optional[datetime.datetime] = None
    result: Optional[datetime.datetime] = None

    async def enqueue(self, q: AsyncIOMotorCollection):
        await q.insert_one(self.dict(by_alias=True))

    async def _update(self, update: Dict[str, Any], q: AsyncIOMotorCollection) -> None:
        await q.update_one({"_id": self.id}, {"$set": update})

    async def _update_with_job_status_end_date_key(
        self, status: JobStatus, date_key: str, q: AsyncIOMotorCollection
    ) -> None:
        await self._update({"status": status, date_key: datetime.datetime.now()}, q)

    async def pending(self, q: AsyncIOMotorCollection):
        await self._update_with_job_status_end_date_key(
            JobStatus.PENDING, Job.STARTED_AT.alias, q
        )

    async def finished(self, result: Any, q: AsyncIOMotorCollection):
        await self._update(
            {
                "status": JobStatus.FINISHED,
                "ended_at": datetime.datetime.now(),
                "result": pickle.dumps(result),
            },
            q,
        )

    async def on_error(self, q: AsyncIOMotorCollection):
        await self._update_with_job_status_end_date_key(
            JobStatus.ON_ERROR, Job.ENDED_AT.alias, q
        )


class JobQueue:
    def __init__(
        self, *, mongo_uri: str, db_name: str = "admin", collection: str = "zusQueue"
    ):
        self.db = AsyncIOMotorClient(mongo_uri)[db_name]
        self.queue_name = collection
        self.q = None

    async def _init(self):
        if not await self._exists():
            await self._create()
        self.q = self.db[self.queue_name]

    @staticmethod
    async def create(*, mongo_uri, db_name, collection):
        job_queue = JobQueue(
            mongo_uri=mongo_uri, db_name=db_name, collection=collection
        )
        await job_queue._init()
        return job_queue

    async def _create(self):
        try:
            logger.info(f"creating queue collection {self.queue_name=}")
            await self.db.create_collection(self.queue_name)
            logger.info(f"creating index on queue collection {self.queue_name=} on field 'status'")
            await self.db[self.queue_name].create_index([("status", ASCENDING)])
        except CollectionInvalid as e:
            raise ValueError(f"Collection {self.queue_name=} already created") from e

    async def _exists(self):
        return self.queue_name in await self.db.list_collection_names()

    async def enqueue(
        self, f: Callable[..., Any] | Coroutine | Job, *args: Any, **kwargs: Any
    ) -> str:
        assert self.q is not None

        if isinstance(f, Job):
            job = f
        else:
            job = Job(f=pickle.dumps((f, args, kwargs)))
            job.id = str(ObjectId())
        await job.enqueue(self.q)
        return job.id


async def dequeue_coro(*, mongo_uri, db_name, collection):
    q = AsyncIOMotorClient(mongo_uri)[db_name][collection]
    logger.debug(f"Started dequeuing process {os.getpid()} on {q.name!r}")
    while True:
        cursor: AsyncIOMotorCursor = q.find({Job.STATUS: JobStatus.WAITING})
        try:
            current_job = Job(**(await cursor.next()))
            # noinspection PyProtectedMember
            logger.debug("Treating row {}", current_job.id)

            await current_job.pending(q)

            loads = pickle.loads
            f, args, kwargs = loads(current_job.f)

            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(f, *args, **kwargs)
                result = future.result()
                if asyncio.iscoroutine(result):
                    try:
                        result = await result
                    except Exception as e:
                        logger.exception(e)
                        await current_job.on_error(q)
                    else:
                        await current_job.finished(result, q)
        except (StopAsyncIteration, StopIteration):
            pass
        except Exception as e:
            logger.exception(e)
        await asyncio.sleep(5)


def syncify(coro, *args, **kwargs):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.create_task(coro(*args, **kwargs), name="dequeuing")
        loop.run_forever()
    except Exception as e:
        logger.exception(e)
        try:
            tasks = asyncio.gather(*asyncio.all_tasks(loop), return_exceptions=True)
            tasks.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                loop.run_until_complete(tasks)
            loop.run_until_complete(loop.shutdown_asyncgens())
        finally:
            loop.close()
        raise e


async def _monitor_dequeuing_process_thread(p: Process) -> None:
    logger.debug("launching thread monitor on main process {}", os.getpid())
    while 1:
        if not p.is_alive():
            logger.debug("process {} is not alive anymore !", p.pid)
            p.close()
            break
        await asyncio.sleep(10)
    logger.debug("Ending monitor thread...")


async def _dequeue_process(*, mongo_uri, db_name, collection):
    logger.debug(f"main process {os.getpid()=}")
    while 1:
        p = multiprocessing.Process(
            target=syncify,
            args=(dequeue_coro,),
            kwargs=dict(mongo_uri=mongo_uri, db_name=db_name, collection=collection),
        )
        p.start()
        maybe_coro = await asyncio.to_thread(_monitor_dequeuing_process_thread, p)
        assert asyncio.iscoroutine(maybe_coro)
        await maybe_coro
        await asyncio.sleep(1)


def dequeue_process_from_job_queue(*, mongo_uri, db_name, collection):
    asyncio.create_task(
        _dequeue_process(mongo_uri=mongo_uri, db_name=db_name, collection=collection)
    )
