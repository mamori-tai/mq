import functools
import multiprocessing
from multiprocessing import Manager

from loguru import logger

from mq._queue import JobQueue, InsertJobResult
from mq._worker import Worker


class P:
    def __init__(self):
        self._manager = Manager()
        self._result_event_by_job_id = self._manager.dict()
        self._cancel_event_by_job_id = self._manager.dict()

    @property
    def result_event_by_job_id(self):
        return self._result_event_by_job_id

    @property
    def cancel_event_by_job_id(self):
        return self._cancel_event_by_job_id

    def close(self):
        self._manager.shutdown()


class MQ:
    job_queue = None
    manager = None
    initialized = False
    p = None

    async def init(self, *, mongo_uri, db_name, collection):
        self.p = multiprocessing.Pool(processes=1)
        self.manager = P()
        self.job_queue = JobQueue(
            mongo_uri=mongo_uri,
            db_name=db_name,
            queue_name=collection,
            manager=self.manager,
        )
        await self.job_queue._init()
        self.initialized = True
        return self.job_queue

    async def enqueue(self, f, *args, **kwargs):
        return self.job_queue.enqueue(f, *args, **kwargs)

    def worker_for(self, channel):
        logger.debug("launching worker")
        return Worker(
            self.job_queue,
            channel,
            self.manager.result_event_by_job_id,
            self.manager.cancel_event_by_job_id,
        )


mq = MQ()


class job:
    def __init__(self, channel):
        self.channel = channel

    def __call__(self, fn):
        @functools.wraps(fn)
        async def _(*args, **kwargs) -> InsertJobResult:
            return await mq.job_queue.enqueue(fn, *args, **kwargs)

        fn.mq = _
        return fn
