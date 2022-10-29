import asyncio
import datetime
import pickle
import typing
from asyncio import CancelledError

from loguru import logger

# noinspection PyProtectedMember
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCursor

from mq import utils
from mq._job import JobStatus, Job
from mq._scheduler import DefaultScheduler
from mq.utils import loads


class TaskRunnerProtocol(typing.Protocol):
    async def run(self, current_job: Job):
        pass


class DefaultTaskRunner:
    async def run(self, current_job: Job) -> typing.Any:
        if current_job.f is None:
            raise TypeError("job function definition does not exist")
        f, args, kwargs = loads(current_job.f)
        if asyncio.iscoroutinefunction(f):
            return await f(*args, **kwargs)
        return await asyncio.to_thread(f, *args, **kwargs)


class RunnerProtocol(typing.Protocol):
    async def dequeue(self):
        pass


class Runner:
    default_scheduler_cls = DefaultScheduler
    scheduler_cls = None
    serializer = utils.dumps
    deserializer = pickle.loads
    task_runner_cls: TaskRunnerProtocol | typing.Callable[
        [Job], typing.Any
    ] = DefaultTaskRunner

    def __init__(
        self,
        task_runner: TaskRunnerProtocol | typing.Callable[[Job], typing.Any] | None,
        worker_id: str,
        mongo_uri: str,
        db_name: str,
        collection: str,
        all_events: dict[str, dict[str, typing.Any]],
        stop_process_event,
    ):
        self._worker_id = worker_id

        self._client = AsyncIOMotorClient(mongo_uri)
        self._q = self._client[db_name][collection]
        self._scheduler = (self.scheduler_cls or self.default_scheduler_cls)(self._q)
        self._worker_queue = self._client[db_name]["mq_workers"]
        self._all_events = all_events
        self._task_runner = task_runner
        self._stop_process_event = stop_process_event
        self._semaphore = asyncio.Semaphore(3)

    def close(self):
        self._client.close()

    @staticmethod
    async def _check_coroutine_function(
        f: typing.Callable[..., typing.Any], *args, **kwargs
    ):
        if not asyncio.iscoroutinefunction(f):
            await asyncio.to_thread(f, *args, **kwargs)
        return await f(*args, **kwargs)

    async def set_job_status(
        self, job_id: str, schedule_job, status: JobStatus, result=None
    ):
        await self._scheduler.schedule(job_id, schedule_job, status, result)

    async def _run_task(self, current_job: Job) -> None:
        async with self._semaphore:
            logger.debug("Running job {}", current_job.id)
            await self._q.find_one_and_update(
                {"_id": current_job.id},
                {"$set": {"started_at": datetime.datetime.utcnow()}},
            )
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
                await self.set_job_status(
                    current_job.id, current_job.schedule, JobStatus.CANCELLED
                )
                # clearing
                event = self._all_events.get("cancel_event_by_job_id").get(
                    current_job.id
                )
                event.clear()
                raise
            except Exception as e:
                logger.exception(e)
                await self.set_job_status(
                    current_job.id, current_job.schedule, JobStatus.ON_ERROR
                )
                raise
            else:
                await self.set_job_status(
                    current_job.id,
                    current_job.schedule,
                    JobStatus.FINISHED,
                    result=self.serializer(result),
                )
            finally:
                self._worker_queue.find_one_and_update(
                    dict(worker_id=self._worker_id), {"$inc": {"nb_tasks": 1}}
                )
            await asyncio.sleep(3)

    async def _task_for_job(self, current_job):
        job_id = current_job.id
        get_event = self._all_events.get
        cancel_event = get_event("cancel_event_by_job_id").get(job_id)
        result_event = get_event("result_event_by_job_id").get(job_id)

        task = asyncio.create_task(self._run_task(current_job))

        cancel_task = asyncio.create_task(asyncio.to_thread(cancel_event.wait))

        def _task_cb(t):
            result_event.set()
            if not t.cancelled():
                if exc := t.exception():
                    raise exc
                cancel_task.cancel()

        task.add_done_callback(_task_cb)

        def _cancel_cb(t):
            if not t.cancelled():
                task.cancel()

        cancel_task.add_done_callback(_cancel_cb)

        await asyncio.wait({task, cancel_task})

    async def dequeue(self):
        tasks = set()
        while True:
            if self._stop_process_event.is_set():
                self.close()
                logger.debug("clearing ")
                self._stop_process_event.clear()
                break

            mongo_query = self._scheduler.mongo_query()
            cursor: AsyncIOMotorCursor = self._q.find(mongo_query)

            try:
                job_as_dict = await cursor.next()
                current_job = Job(**job_as_dict)

                logger.debug("Treating row {}", current_job.id)
                # updating directly avoid resending a new job
                await self._q.find_one_and_update(
                    {"_id": current_job.id},
                    {
                        "$set": {
                            "status": JobStatus.PENDING,
                            "locked_by": self._worker_id,
                        }
                    },
                )
                task = asyncio.create_task(self._task_for_job(current_job))
                task.add_done_callback(tasks.discard)

            except (StopAsyncIteration, StopIteration):
                pass
            except CancelledError as e:
                raise e
            except Exception as e:
                logger.exception(e)
                raise e
