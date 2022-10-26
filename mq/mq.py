import asyncio
import functools
import threading
import weakref
from multiprocessing import Manager, current_process
from multiprocessing.managers import SyncManager
from typing import Callable, Any

import schedule
from loguru import logger

from mq._job import Job
from mq._queue import JobQueue, JobCommand
from mq._runner import TaskRunnerProtocol, Runner, RunnerProtocol
from mq._scheduler import SchedulerProtocol, DefaultScheduler
from mq._worker import Worker
from mq.utils import MongoDBConnectionParameters, MQManagerConnectionParameters


class MQManager(SyncManager):
    pass


class P:
    """
    Shared memory to handle getting result or cancel job
    """

    result_key = "result_event_by_job_id"
    cancel_key = "cancel_event_by_job_id"

    def __init__(self):
        self._manager = Manager()
        self._events_by_job_id = self._manager.dict(
            {
                self.result_key: self._manager.dict(),
                self.cancel_key: self._manager.dict(),
            }
        )

        self._close_manager_event = threading.Event()

    def all_events(self):
        return self._events_by_job_id

    @property
    def result_event_by_job_id(self):
        return self._events_by_job_id[self.result_key]

    @property
    def cancel_event_by_job_id(self):
        return self._events_by_job_id[self.cancel_key]

    def event(self):
        return self._manager.Event()

    def init_events_for_job_id(self, job_id: str):
        self.result_event_by_job_id[job_id] = self._manager.Event()
        self.cancel_event_by_job_id[job_id] = self._manager.Event()

    def init_cancel_event_for_worker_id(self, worker_id: str):
        self.cancel_event_by_job_id[worker_id] = self._manager.Event()

    def close(self):
        logger.debug("Closing manager...")
        self._manager.shutdown()


def get_events_by_job_id(events_by_job_id):
    return events_by_job_id


def init_cancel_event_for_worker_id(events_by_job_id, ev, worker_id: str):
    events_by_job_id["cancel_event_by_job_id"][worker_id] = ev


def start_mq_manager_server(
    address, authkey, events_by_job_id, close_manager_event, ev_creator
):
    MQManager.register(
        "events_by_job_id", functools.partial(get_events_by_job_id, events_by_job_id)
    )
    MQManager.register(
        "init_cancel_event_for_worker_id",
        functools.partial(
            init_cancel_event_for_worker_id, events_by_job_id, ev_creator()
        ),
    )
    manager = MQManager(address=address, authkey=authkey)
    manager.start()
    close_manager_event.wait()
    manager.shutdown()


class MQ:
    """ """

    job_queue: JobQueue | None = None
    shared_memory: P | None = None
    initialized: bool = False
    workers: weakref.WeakValueDictionary[str, Worker] = {}
    server_activated: bool = False
    mq_server_params: MQManagerConnectionParameters | None = None

    runner_cls: Callable[[...], RunnerProtocol] = Runner
    scheduler_cls: Callable[[...], SchedulerProtocol] = DefaultScheduler
    task_runner_by_channel: dict[str, Callable[[...], TaskRunnerProtocol] | Callable[[Job], Any]] = {}

    def with_process_connection(self, mq_server_params: MQManagerConnectionParameters):
        self.mq_server_params = mq_server_params
        current_process().authkey = mq_server_params.authkey
        return self

    def _start_server(self):
        asyncio.create_task(
            asyncio.to_thread(
                start_mq_manager_server,
                (self.mq_server_params.url, self.mq_server_params.port),
                self.mq_server_params.authkey,
                self.shared_memory._events_by_job_id,
                self.shared_memory._close_manager_event,
                self.shared_memory._manager.Event,
            )
        )

    async def init(
        self,
        mongodb_connection: MongoDBConnectionParameters,
        start_server: bool = False,
    ):
        self.shared_memory = P()

        if start_server:
            assert self.mq_server_params is not None
            self._start_server()
            self.server_activated = True

        self.job_queue = JobQueue(
            mongodb_connection=mongodb_connection,
            shared_memory=self.shared_memory,
        )
        await self.job_queue.init()

        self.initialized = True
        logger.debug("MQ instance parametrized")
        return self

    async def enqueue(self, f=None, *args, **kwargs):
        """
        direct enqueuing
        Args:
            f:
            *args:
            **kwargs:

        Returns:

        """
        return await self.job_queue.enqueue(f, *args, **kwargs)

    def worker_for(self, *, channel: str = "default"):
        logger.debug("launching worker for {}", channel)
        worker = Worker(
            self.job_queue.connection_parameters,
            channel=channel,
            all_events=self.shared_memory.all_events(),
            runner_cls=self.runner_cls,
            scheduler_cls=self.scheduler_cls,
            task_runner=self.task_runner_by_channel.get(channel)
        )
        worker.init_cancel_event(self.shared_memory.event())
        self.register_worker(worker)
        return worker

    def distant_worker(
        self,
        *,
        channel: str = "default",
    ):
        logger.debug("launching worker for {}", channel)
        assert (
            self.mq_server_params is not None
        ), "Can not connect to distant manager server..."
        worker = Worker(
            self.job_queue.connection_parameters,
            channel=channel,
            mq_manager_parameters=self.mq_server_params,
            runner_cls=self.runner_cls,
            scheduler_cls=self.scheduler_cls,
            task_runner=self.task_runner_by_channel.get(channel)
        )
        worker.connect()
        self.register_worker(worker)
        return worker

    def register_worker(self, worker):
        self.workers[worker._worker_id] = worker


mq = MQ()


class mongodb_connection_parameters:
    def __init__(self, *, mongo_uri: str, db_name: str, collection: str):
        self._mongodb_connection = MongoDBConnectionParameters(
            mongo_uri=mongo_uri, db_name=db_name, collection=collection
        )

    async def __aenter__(self):
        return await mq.init(self._mongodb_connection)

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await asyncio.sleep(4)
        mq.shared_memory.close()
        mq.shared_memory._close_manager_event.set()


class job:
    """ """

    def __init__(self, *, channel: str, schedule: schedule.Job = None):
        self._channel = channel
        self._schedule = schedule

    def __call__(self, fn):
        @functools.wraps(fn)
        async def _(*args, **kwargs) -> JobCommand:
            try:
                return await mq.job_queue.enqueue(fn, *args, **kwargs)
            except Exception as e:
                logger.exception(e)
                raise

        fn.mq = _
        return fn


def nope(fn):
    def _():
        return fn
    return _

class register_task_runner:
    def __init__(self, *, channel: str):
        self._channel = channel

    def __call__(self, fn):
        mq.task_runner_by_channel[self._channel] = fn
        return fn


every = schedule.every
