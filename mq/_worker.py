import asyncio
import multiprocessing
import pickle
from asyncio import CancelledError

from loguru import logger
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCursor

from mq._job import Job, JobStatus
from mq._queue import JobQueue
from mq.utils import _cancel_all_tasks


class Worker:
    def __init__(
        self,
        queue: JobQueue,
        channel: str,
        result_event_by_job_id,
        cancel_event_by_job_id,
    ):
        self._channel = channel
        self._tasks = set()
        self._mongo_uri = queue._mongo_uri
        self._db_name = queue._db_name
        self._collection = queue._queue_name
        self.cancel_event_by_job_id = cancel_event_by_job_id
        self.result_event_by_job_id = result_event_by_job_id

    async def _run_task(self, q, current_job, f, *args, **kwargs):
        try:
            result = f(*args, **kwargs)
            if asyncio.iscoroutine(result):
                result = await result
        except CancelledError:
            await q.find_one_and_update(
                {"_id": current_job.id}, {"$set": {"status": "cancelled"}}
            )
            raise
        except Exception as e:
            logger.exception(e)
            await current_job.on_error(q)
        else:
            await current_job.finished(result, q)

    async def dequeue(self):
        q = AsyncIOMotorClient(self._mongo_uri)[self._db_name][self._collection]
        while True:
            cursor: AsyncIOMotorCursor = q.find({"status": JobStatus.WAITING})
            try:
                job_as_dict = await cursor.next()
                current_job = Job(**(job_as_dict))
                # noinspection PyProtectedMember
                logger.debug("Treating row {}", current_job.id)

                await current_job.pending(q)

                loads = pickle.loads
                f, args, kwargs = loads(current_job.f)
                task = asyncio.create_task(
                    self._run_task(q, current_job, f, *args, **kwargs)
                )
                task.add_done_callback(self._tasks.discard)
                task.add_done_callback(
                    lambda _: self.result_event_by_job_id[current_job.id].set()
                )
                cancel_task = asyncio.create_task(
                    asyncio.to_thread(self.cancel_event_by_job_id[current_job.id].wait)
                )
                done, pending = await asyncio.wait(
                    {task, cancel_task}, return_when=asyncio.FIRST_COMPLETED
                )
                cancelled = cancel_task in done
                if cancelled:
                    for p in pending:
                        p.cancel()
            except (StopAsyncIteration, StopIteration):
                pass
            except Exception as e:
                logger.exception(e)
            await asyncio.sleep(5)

    def syncify(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.create_task(self.dequeue(), name="dequeuing")
            loop.run_forever()
        except Exception as e:
            logger.exception(e)
            _cancel_all_tasks(loop)
            raise e

    def start(self):
        p = multiprocessing.Pool(processes=1)
        p.apply_async(self.syncify)
        p.close()
