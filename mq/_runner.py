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

from mq import utils
from mq._job import Job, JobStatus
from mq._queue import JobCommand
from mq._scheduler import DefaultScheduler, SchedulerProtocol
from mq.utils import EnqueueMixin, MongoDBConnectionParameters, loads


class TaskRunnerProtocol(typing.Protocol):
    async def run(self, current_job: Job):
        pass


class DefaultTaskRunner:
    async def run(self, current_job: Job, enqueue: typing.Any | None) -> typing.Any:
        if current_job.f is None:
            raise TypeError("job function definition does not exist")
        f, args, kwargs = loads(current_job.f)
        if asyncio.iscoroutinefunction(f):
            return await f(*args, **kwargs)
        return await asyncio.to_thread(f, *args, **kwargs)


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

    async def enqueue(
        self,
        f: typing.Callable[..., typing.Any] | typing.Coroutine | None,
        *args: typing.Any,
        **kwargs: typing.Any,
    ):
        job = await self.enqueue_job(f, *args, **kwargs)
        self.events[job.id] = self.manager.list(
            self.manager.Event(), self.manager.Event()
        )
        return JobCommand(job_id=job._id, q=self.q, events=self.events)

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
                result = await self._check_coroutine_function(
                    f, current_job, self.enqueue
                )
            except CancelledError:
                logger.debug("task cancelled {} !", current_job.id)
                await self.set_job_status(current_job, JobStatus.CANCELLED)
                # clearing
                event = self.events.get(current_job.id)[1]
                event.clear()

                del self.events[current_job.id]
                raise
            except Exception as e:
                logger.exception(e)
                await self.set_job_status(current_job, JobStatus.ON_ERROR)
                raise
            else:
                await self.set_job_status(
                    current_job,
                    JobStatus.FINISHED,
                    result=self.serializer(result),
                )
            finally:
                self._wq.find_one_and_update(
                    dict(worker_id=self._worker_id), {"$inc": {"nb_tasks": 1}}
                )

    async def wait(self):
        await asyncio.sleep(3)

    async def _task_for_job(self, current_job: Job):
        job_id = current_job.id
        get_event = self.events.get
        cancel_event = get_event(job_id)[1]
        result_event = get_event(job_id)[0]

        task = asyncio.create_task(self._run_task(current_job))

        cancel_task = (
            asyncio.create_task(asyncio.to_thread(cancel_event.wait))
            if cancel_event is not None
            else None
        )

        def _task_cb(t):
            if result_event is not None:
                result_event.set()
            if not t.cancelled():
                if exc := t.exception():
                    logger.exception(exc)
                    raise exc
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
                worker_stop_event.clear()
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
                raise
            except Exception as e:
                logger.debug("Unknown error, runner cancelled")
                logger.exception(e)
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
