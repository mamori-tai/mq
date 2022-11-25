import asyncio
import functools
import threading
import uuid
import weakref
from multiprocessing import Manager, current_process
from multiprocessing.managers import SyncManager
from typing import Any, Callable

# noinspection PyPep8Naming
import schedule as JobSchedule
from loguru import logger

from mq._job import Job
from mq._queue import JobCommand, JobQueue
from mq._runner import DefaultRunner, RunnerProtocol, TaskRunnerProtocol
from mq._scheduler import DefaultScheduler, SchedulerProtocol
from mq._worker import Worker
from mq.utils import MongoDBConnectionParameters, MQManagerConnectionParameters


class MQManager(SyncManager):
    pass


class P:
    """
    Shared memory to handle getting result or cancel job
    """

    def __init__(self):
        self.manager = Manager()
        self._events_by_job_id = self.manager.dict()

        #
        self.mq_server_close_event = threading.Event()

    def events(self):
        return self._events_by_job_id

    def close(self):
        logger.debug("Closing shared memory P manager...")
        self.manager.shutdown()


def get_events_by_job_id(events_by_job_id):
    return events_by_job_id


def start_mq_manager_server(address, authkey, events_by_job_id, close_manager_event):
    MQManager.register(
        "events", functools.partial(get_events_by_job_id, events_by_job_id)
    )
    manager = MQManager(address=address, authkey=authkey)
    manager.start()
    close_manager_event.wait()
    manager.shutdown()


class MQ:
    """
    Main class
    """

    job_queue: JobQueue | None = None
    shared_memory: P | None = None
    initialized: bool = False
    workers: weakref.WeakValueDictionary[str, Worker] = {}
    server_activated: bool = False
    mongodb_connection_params: MongoDBConnectionParameters | None = None
    mq_server_params: MQManagerConnectionParameters | None = None
    in_worker_process: bool = False
    # client_manager: MQManager | None = None
    client_activated: bool = False

    scheduler: SchedulerProtocol = None
    task_runner_by_channel: dict[str, TaskRunnerProtocol | Callable[[Job], Any]] = {}

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
                self.shared_memory.events(),
                self.shared_memory.mq_server_close_event,
            )
        )

    def _connect(self):
        if self.mq_server_params is None:
            raise ValueError("Cannot connect to distant server")

        MQManager.register("events")

        authkey = self.mq_server_params.authkey
        self.client_manager = MQManager(
            address=(self.mq_server_params.url, self.mq_server_params.port),
            authkey=authkey,
        )
        self.client_manager.connect()

    async def init(
        self,
        mongodb_connection_params: MongoDBConnectionParameters,
        start_server: bool = False,
        in_worker_process: bool = False,
        scheduler: SchedulerProtocol = DefaultScheduler(),
    ):
        self.mongodb_connection_params = mongodb_connection_params
        self.scheduler = scheduler
        self.shared_memory = P()

        if start_server:
            assert self.mq_server_params is not None
            self._start_server()
            self.server_activated = True

        if in_worker_process:
            self._connect()
            self.client_activated = True

        self.job_queue = JobQueue(
            mongodb_connection=mongodb_connection_params,
            shared_memory=self.shared_memory,
            scheduler=self.scheduler,
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

    @property
    def is_in_worker_process(self):
        return False  # .is_in_worker_process
        # return self.client_manager is not None

    @property
    def events(self):
        if self.is_in_worker_process:
            # noinspection PyUnresolvedReferences
            return self.client_manager.events()

        return self.shared_memory.events()

    def _init_worker_stop_event(self, worker_id: str) -> None:
        events = self.events
        manager = (
            self.client_manager
            if self.is_in_worker_process
            else self.shared_memory.manager
        )
        events[worker_id] = manager.Event()

    def gen_new_worker(
        self,
        worker_id: str = None,
        runner: RunnerProtocol = None,
        nb_processes: int = 1,
    ) -> Worker:
        worker_id = worker_id or str(uuid.uuid4())
        self._init_worker_stop_event(worker_id)
        worker = Worker(
            db=self.job_queue.db,
            worker_id=worker_id,
            nb_processes=nb_processes,
            runner=runner,
            parent_manager=self.shared_memory.manager,
            worker_stop_event=self.events.get(worker_id),
        )
        self.register_worker(worker)
        return worker

    def default_worker(
        self,
        *,
        channel: str = "default",
        nb_processes: int = 1,
        max_concurrency: int = 3,
        dequeuing_delay: int = 3,
        task_runner=None,
    ) -> Worker:
        worker_id = str(uuid.uuid4())
        worker = self.gen_new_worker(worker_id=worker_id, nb_processes=nb_processes)
        worker._runner = DefaultRunner(
            channel=channel,
            worker_id=worker_id,
            max_concurrency=max_concurrency,
            dequeuing_delay=dequeuing_delay,
            scheduler=self.scheduler,
            mongodb_connection_params=self.mongodb_connection_params,
            events=self.events,
            task_runner=task_runner or self.task_runner_by_channel.get(channel),
        )
        return worker

    def worker_with_runner(self, *, runner: RunnerProtocol, nb_processes: int = 1):
        # ensure init has been called
        assert self.scheduler is not None
        if runner.scheduler is not None:
            runner_scheduler = type(runner.scheduler)
            self_scheduler = type(self.scheduler)
            if runner_scheduler is not self_scheduler:
                raise ValueError("Scheduler not matching between runner and mq")
        else:
            runner.scheduler = self.scheduler

        return self.gen_new_worker(None, runner, nb_processes)

    def register_worker(self, worker: Worker):
        self.workers[worker.worker_id] = worker


mq = MQ()


# noinspection PyPep8Naming
class mongodb_connection_parameters:
    def __init__(self, *, mongo_uri: str, db_name: str, collection: str):
        self._mongodb_connection = MongoDBConnectionParameters(
            mongo_uri=mongo_uri, db_name=db_name, collection=collection
        )

    async def __aenter__(self):
        return await mq.init(self._mongodb_connection)

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        mq.shared_memory.close()
        mq.shared_memory.mq_server_close_event.set()


# noinspection PyPep8Naming
class job:
    """ """

    def __init__(
        self,
        *,
        channel: str,
        schedule: JobSchedule.Job = None,
        downstream: list[Any] = None,
        # supporting only these attributes
        stop=None,
        wait=None,
        retry=None
    ):
        self._channel = channel
        self._schedule = schedule
        self._downstream = downstream
        self._retry = retry
        self._stop = stop
        self._wait = wait

    def __call__(self, fn):
        @functools.wraps(fn)
        async def _(*args, **kwargs) -> JobCommand:
            try:
                return await mq.job_queue.enqueue(fn, *args, **kwargs)
            except Exception as e:
                logger.exception(e)
                raise

        fn.mq = _
        fn._downstream = self._downstream
        fn._schedule = self._schedule
        fn._retry = self._retry
        fn._stop = self._stop
        self._wait = self._wait
        return fn


# noinspection PyPep8Naming
class register_task_runner:
    def __init__(self, *, channel: str):
        self._channel = channel

    def __call__(self, fn):
        mq.task_runner_by_channel[self._channel] = fn
        return fn


every = JobSchedule.every
