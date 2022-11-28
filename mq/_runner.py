import asyncio
import dataclasses
import multiprocessing
import threading
import typing
from asyncio import CancelledError
from multiprocessing.managers import SyncManager

from loguru import logger

# noinspection PyProtectedMember
from motor.motor_asyncio import (
    AsyncIOMotorClient,
    AsyncIOMotorCollection,
    AsyncIOMotorCursor,
)
from tenacity import AsyncRetrying, RetryError

from mq import utils
from mq._job import Job, JobStatus
from mq._scheduler import DefaultScheduler, SchedulerProtocol
from mq.utils import EnqueueMixin, MongoDBConnectionParameters, loads


class TaskRunnerProtocol(typing.Protocol):
    async def run(self, current_job: Job):
        pass


class DefaultTaskRunner:
    async def _run(self, f, *args, **kwargs):
        if asyncio.iscoroutinefunction(f):
            return await f(*args, **kwargs)
        # if not a coroutine function launching in a thread
        return await asyncio.to_thread(f, *args, **kwargs)

    async def run(self, current_job: Job) -> typing.Any:
        if current_job.f is None:
            raise TypeError("job function definition does not exist")
        f, args, kwargs = loads(current_job.f)
        retry = current_job.extra.get("retry")
        if retry is not None:
            retry = {k: v for k, v in {k: loads(v) for k, v in retry.items()}.items() if v is not None}
        if retry is not None:
            async for attempt in AsyncRetrying(**retry):
                logger.debug("retrying...")
                with attempt:
                    return await self._run(f, *args, **kwargs)
            return

        return await self._run(f, *args, **kwargs)


class RunnerProtocol(typing.Protocol):

    scheduler: SchedulerProtocol = None

    async def post_init(self):
        ...

    async def dequeue(self):
        ...


class DefaultRunner(EnqueueMixin):
    serializer = utils.dumps
    deserializer = utils.loads
    task_runner_cls: TaskRunnerProtocol | typing.Callable[
        [Job], typing.Any
    ] = DefaultTaskRunner

    def __init__(
        self,
        *,
        channel: str = None,
        scheduler: SchedulerProtocol = None,
        task_runner: TaskRunnerProtocol
        | typing.Callable[[Job], typing.Any]
        | None = None,
        worker_id: str | None = None,
        worker_queue: str = "mq_workers",
        mongodb_connection_params: MongoDBConnectionParameters = None,
        events: dict[str, list[threading.Event] | threading.Event] | None = None,
        max_concurrency: int = 3,
        dequeuing_delay: int = 3,
    ):
        self._worker_id = worker_id
        self._channel = channel
        self._mongodb_connection_params = mongodb_connection_params
        self._worker_queue = worker_queue

        self._client: AsyncIOMotorClient | None = None
        self.q: AsyncIOMotorCollection | None = None
        self._wq: AsyncIOMotorCollection | None = None

        self.events = events

        self.scheduler = scheduler or DefaultScheduler()
        self._task_runner = task_runner

        self._dequeuing_delay = dequeuing_delay
        self._max_concurrency = max_concurrency

        self._semaphore = asyncio.Semaphore(self._max_concurrency)
        self.manager: SyncManager | None = None
        self._tasks = set()

    @staticmethod
    async def _check_coroutine_function(
        f: typing.Callable[..., typing.Any], *args, **kwargs
    ):
        if not asyncio.iscoroutinefunction(f):
            await asyncio.to_thread(f, *args, **kwargs)
        return await f(*args, **kwargs)

    async def set_job_status(
        self, job: Job, status: JobStatus, result: typing.Any = None
    ):

        is_scheduled = self.scheduler.schedule_job(job)

        computed_status = status
        if computed_status != JobStatus.CANCELLED:
            computed_status = status if not is_scheduled else JobStatus.WAITING

        await self.q.find_one_and_update(
            {"_id": job.id},
            {
                "$set": dataclasses.asdict(job)
                | dict(result=result, status=computed_status)
            },
        )

    async def cancel_downstream(self, computed_downstream: dict[str, typing.Any]):
        "should be a mixin to be provided by job command"
        for job_id, child in computed_downstream.items():
            self.q.find_one_and_update(
                {"_id": job_id}, {"$set": {"status": JobStatus.CANCELLED}}
            )
            await self.cancel_downstream(child)

    async def set_downstream_job_status(
        self, computed_downstream: dict[str, typing.Any], result: typing.Any = None
    ):
        for job_id in computed_downstream.keys():
            job = Job(**await self.q.find_one({"_id": job_id}))
            f, args, kwargs = loads(job.f)
            self.q.find_one_and_update(
                {"_id": job_id, "status": JobStatus.WAITING_FOR_UPSTREAM},
                {
                    "$set": {
                        "status": JobStatus.WAITING,
                        "f": self.serializer((f, (result,), {})),
                    }
                },
            )

    async def _run_task(self, current_job: Job) -> None:
        async with self._semaphore:

            logger.debug("Running job {}", current_job.id)

            try:
                # if f is defined fallback to default task runner
                if current_job.f is not None:
                    f = DefaultTaskRunner().run
                else:
                    if getattr(self._task_runner, "run", None) is not None:
                        f = self._task_runner.run
                    else:
                        f = self._task_runner
                result = await self._check_coroutine_function(f, current_job)
            except CancelledError:
                logger.debug("task cancelled {} !", current_job.id)
                await self.set_job_status(current_job, JobStatus.CANCELLED)
                await self.cancel_downstream(
                    computed_downstream=current_job.computed_downstream
                )
                raise
            except Exception as e:
                if not isinstance(e, RetryError):
                    logger.exception(e)
                await self.set_job_status(current_job, JobStatus.ON_ERROR)
                await self.cancel_downstream(
                    computed_downstream=current_job.computed_downstream
                )
                raise
            else:
                await self.set_job_status(
                    current_job,
                    JobStatus.FINISHED,
                    result=self.serializer(result),
                )
                await self.set_downstream_job_status(
                    current_job.computed_downstream, result=result
                )
            finally:
                logger.debug("Task {} finished", current_job.id)
                self._wq.find_one_and_update(
                    dict(worker_id=self._worker_id), {"$inc": {"nb_tasks": 1}}
                )
                event_result = self.events.get(current_job.id)[0]
                event_result.set()

    async def wait(self):
        await asyncio.sleep(3)

    async def _task_for_job(self, current_job: Job):
        job_id = current_job.id
        get_event = self.events.get
        cancel_event = get_event(job_id)[1]

        task = asyncio.create_task(self._run_task(current_job))

        cancel_task = (
            asyncio.create_task(asyncio.to_thread(cancel_event.wait))
            if cancel_event is not None
            else None
        )

        def _task_cb(t):
            if not t.cancelled():
                if cancel_task is not None:
                    cancel_task.cancel()

        task.add_done_callback(_task_cb)

        def _cancel_cb(t):
            if not t.cancelled():
                if exc := t.exception():
                    logger.exception(exc)
                task.cancel()

        if cancel_task is not None:
            cancel_task.add_done_callback(_cancel_cb)

        if cancel_task is None:
            await task
            return
        await asyncio.wait({task, cancel_task})
        await self.wait()

    async def _dequeue(self):
        worker_stop_event = self.events.get(self._worker_id)
        while True:
            if worker_stop_event.is_set():
                logger.debug("worker required to stop")
                self._client.close()
                break
            mongo_query = self.scheduler.mongo_query()
            cursor: AsyncIOMotorCursor = self.q.find(mongo_query)
            try:
                job_as_dict = await cursor.next()
                current_job = Job(**job_as_dict)

                logger.debug("Treating row {}", current_job._id)
                current_job.locked_by = self._worker_id
                # updating directly avoid resending a new job
                await self.q.find_one_and_update(
                    {"_id": current_job._id},
                    {
                        "$set": {
                            "status": JobStatus.PENDING,
                            "locked_by": self._worker_id,
                        }
                    },
                )
                yield current_job
            except (StopAsyncIteration, StopIteration):
                pass
            except CancelledError:
                logger.debug("Runner cancelled")
                worker_stop_event.clear()
                raise
            except Exception as e:
                logger.debug("Unknown error, runner cancelled")
                logger.exception(e)
                worker_stop_event.clear()
                raise

    async def post_init(self) -> None:
        """
        Setup db enabling pickling the runner
        Returns:

        """
        self._client = AsyncIOMotorClient(self._mongodb_connection_params.mongo_uri)
        db = self._client[self._mongodb_connection_params.db_name]
        self.q = db[self._mongodb_connection_params.collection]
        self._wq = db[self._worker_queue]

        self.manager = multiprocessing.Manager()

    async def dequeue(self) -> None:
        """
        Dequeuing job and start task on it
        Returns:

        """
        tasks = set()
        await self.post_init()
        try:
            async for current_job in self._dequeue():
                task = asyncio.create_task(self._task_for_job(current_job))
                task.add_done_callback(tasks.discard)
        finally:
            logger.debug("Shutting down manager")
            self.manager.shutdown()
            self.manager = None
