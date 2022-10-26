import asyncio
import pickle
import typing
from asyncio import CancelledError

from loguru import logger
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
        result = f(*args, **kwargs)
        if asyncio.iscoroutine(result):
            result = await result
        return result


class Runner:
    schedule_cls = DefaultScheduler
    serializer = utils.dumps
    deserializer = pickle.loads
    task_runner_cls: typing.Type[TaskRunnerProtocol] = DefaultTaskRunner

    def __init__(
        self,
        task_runner: typing.Type[TaskRunnerProtocol] | None,
        worker_id: str,
        mongo_uri: str,
        db_name: str,
        collection: str,
        all_events: dict[str, dict[str, typing.Any]],
        stop_process_event,
    ):
        self._worker_id = worker_id
        self._scheduler = self.schedule_cls()
        self._client = AsyncIOMotorClient(mongo_uri)
        self._q = self._client[db_name][collection]
        self._all_events = all_events
        self._task_runner_cls = task_runner if task_runner else self.task_runner_cls
        self._stop_process_event = stop_process_event
        self._semaphore = asyncio.Semaphore(3)

    def close(self):
        self._client.close()

    async def _run_task(self, current_job: Job) -> None:
        async with self._semaphore:
            logger.debug("Running job {}", current_job.id)
            try:
                # if f is defined fallback to default task runner
                if current_job.f is not None:
                    result = await DefaultTaskRunner().run(current_job)
                else:
                    if getattr(self._task_runner_cls, "run", None) is not None:
                        result = await self._task_runner_cls.run(current_job)
                    else:
                        result = self._task_runner_cls(current_job)
            except CancelledError:
                logger.debug("task cancelled {} !", current_job.id)
                await self._q.find_one_and_update(
                    {"_id": current_job.id}, {"$set": {"status": JobStatus.CANCELLED}}
                )
                # clearing
                event = self._all_events.get("cancel_event_by_job_id").get(current_job.id)
                event.clear()
                raise
            except Exception as e:
                logger.exception(e)
                await self._q.find_one_and_update(
                    {"_id": current_job.id}, {"$set": {"status": JobStatus.ON_ERROR}}
                )
            else:
                await self._q.find_one_and_update(
                    {"_id": current_job.id},
                    {
                        "$set": {
                            "status": JobStatus.FINISHED,
                            "result": self.serializer(result),
                        }
                    },
                )
            await asyncio.sleep(3)

    async def task_for_job(self, current_job):
        job_id = current_job.id
        get_event = self._all_events.get
        cancel_event = get_event("cancel_event_by_job_id").get(job_id)
        result_event = get_event("result_event_by_job_id").get(job_id)

        task = asyncio.create_task(
            self._run_task(current_job)
        )

        cancel_task = asyncio.create_task(
            asyncio.to_thread(cancel_event.wait)
        )

        def _task_cb(t):
            result_event.set()
            if not t.cancelled():
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
                return
            cursor: AsyncIOMotorCursor = self._q.find({"status": JobStatus.WAITING})
            try:
                job_as_dict = await cursor.next()
                current_job = Job(**job_as_dict)

                logger.debug("Treating row {}", current_job.id)
                # updating directly avoid resending a new job
                await self._q.find_one_and_update(
                    {"_id": current_job.id}, {"$set": {"status": JobStatus.PENDING}}
                )
                task = asyncio.create_task(self.task_for_job(current_job))
                task.add_done_callback(tasks.discard)

            except (StopAsyncIteration, StopIteration):
                pass
            except CancelledError as e:
                logger.exception(e)
                raise e
            except Exception as e:
                logger.exception(e)
