import asyncio
import concurrent.futures
import threading
import typing
from asyncio import CancelledError
from typing import Any, Callable, Coroutine

import async_timeout
import pymongo
from loguru import logger

# noinspection PyProtectedMember
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection
from pymongo.errors import CollectionInvalid
from pymongo.results import DeleteResult

from mq._job import Job, JobStatus
from mq._scheduler import SchedulerProtocol
from mq.utils import (
    CancelDownstreamJobMixin,
    EnqueueMixin,
    MongoDBConnectionParameters,
    loads,
)

if typing.TYPE_CHECKING:
    from mq.mq import P


class JobCancelledError(Exception):
    ...


class DeleteJobError(Exception):
    ...


class JobCommand(CancelDownstreamJobMixin):
    """
    Provides simple command to interact with enqueued job
    """

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

    async def delete(self) -> DeleteResult | typing.NoReturn:
        """
        Allow deleting the job and associated events in shared memory
        Returns:

        """
        job = await self.job(as_job=True)
        if job.status not in {
            JobStatus.CANCELLED,
            JobStatus.ON_ERROR,
            JobStatus.FINISHED,
        }:
            raise DeleteJobError(f"Job id {job.id} in status {job.status}")
        # delete from all events
        del self.events[job.id]

        # delete from database
        return await self.q.delete_one({"_id": job.id})

    def command_for(self, downstream_id: str) -> "JobCommand":
        """
        Returns a JobCommand for a downstream job to perform operations
        on it
        Args:
            downstream_id: str the id of the downstream job

        Returns:
            JobCommand
        """
        return JobCommand(downstream_id, self.q, self.events)

    async def leaves(self, leaves=None):
        """
        return all leaves i.e. job id of a Directed Acyclic Graph
        Args:
            leaves: None (used in recursive manner)

        Returns:
            list[str] all job_id of job leaves
        """
        if leaves is None:
            leaves = []
        job_as_dict = await self.job(as_job=True)
        jobs = job_as_dict.computed_downstream
        for v, child in jobs.items():
            if not child:
                leaves.append(v)
            job_command = JobCommand(v, self.q, self.events)
            await job_command.leaves(leaves)
        return leaves

    async def cancel(self) -> bool:
        """
        Try to cancel a job even if ti is running
        Returns:
            bool cancelling success
        """
        doc = await self.q.find_one_and_update(
            {
                "_id": self._job_id,
                "status": {"$in": [JobStatus.WAITING, JobStatus.WAITING_FOR_UPSTREAM]},
            },
            {"$set": {"status": JobStatus.CANCELLED}},
        )
        if doc is not None:
            logger.debug("Cancelling downstream job...")
            await self.cancel_downstream(computed_downstream=doc["computed_downstream"])
            return True

        ev = self.events.get(self._job_id)[1]
        if ev is None:
            raise ValueError("Could not find event")

        ev.set()
        logger.debug("Cancelling downstream job...")
        await self.cancel_downstream(
            computed_downstream=(await self.job())["computed_downstream"]
        )
        return (await self.job())["status"] == JobStatus.CANCELLED

    async def wait_for_result(self, timeout: float | None = None) -> Any:
        """
        wait for the result of the job
        Args:
            timeout: float time to wait after returning. If None wait forever

        Returns:
            result: Any the result of the job
        """
        event_result, event_cancel = self.events.get(self._job_id)
        if event_cancel.is_set():
            raise JobCancelledError(f"Job id ${self._job_id} has been cancelled")

        refreshed_job = await self.job()
        if refreshed_job["status"] == JobStatus.CANCELLED:
            raise JobCancelledError(
                f"Job id ${refreshed_job['_id']} has been cancelled"
            )

        event = self.events.get(self._job_id)[0]
        if event is None:
            raise ValueError("Could not find event")

        executor = concurrent.futures.ThreadPoolExecutor()
        try:
            async with async_timeout.timeout(timeout):
                await asyncio.get_running_loop().run_in_executor(executor, event.wait)
        except asyncio.TimeoutError:
            return None
        finally:
            executor.shutdown()

        refreshed_job = await self.job()
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
        """
        Add a callback when a job is done (even if it failed)
        Args:
            cb: Callable | Coroutine

        Returns:
            None
        """
        task = asyncio.get_running_loop().create_task(self.wait_for_result())
        task.add_done_callback(lambda t: self._done_cb(t, cb))


class JobQueue(EnqueueMixin):
    """
    Job queue class which enqueues some jobs
    """

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
        """

        Returns:
            MongoDBConnectionParameters

        """
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
        """
        Enqueue a function in mongo
        Args:
            f:
            *args:
            **kwargs:

        Returns:
            JobCommand instance
        """
        events = self._shared_memory.events()
        job = await self.enqueue_job(
            job_id=None,
            status=JobStatus.WAITING,
            downstream_job={},
            events=events,
            manager=self._shared_memory.manager,
            f=(f, args, kwargs),
        )
        # returning the job command
        # noinspection PyProtectedMember
        return JobCommand(job_id=job._id, q=self.q, events=events)
