import asyncio
import datetime
import multiprocessing
import signal
import typing
import uuid
from enum import IntEnum
from functools import partial
from multiprocessing.managers import SyncManager
from typing import Any

from loguru import logger
from motor.motor_asyncio import AsyncIOMotorClient

from mq._job import Job
from mq._runner import TaskRunnerProtocol, RunnerProtocol
from mq.utils import (
    wait_for_event_cleared,
    MongoDBConnectionParameters,
    MQManagerConnectionParameters,
)


def syncify(coroutine_function, *args, **kwargs):
    """

    Args:
        coroutine_function:
        *args:
        **kwargs:

    Returns:

    """
    asyncio.run(coroutine_function(*args, **kwargs))


def build_runner(
    runner_cls: typing.Type[RunnerProtocol],
    task_runner: TaskRunnerProtocol | typing.Callable[[Job], Any],
    *args,
    **kwargs,
):
    runner = runner_cls(task_runner, *args, **kwargs)
    return runner.dequeue()


class MQManager(SyncManager):
    pass


class WorkerStatus(IntEnum):
    TERMINATED = 0
    RUNNING = 1


class Worker:
    collection = "mq_workers"

    def __init__(
        self,
        connection_parameters: MongoDBConnectionParameters,
        *,
        channel: str,
        all_events: dict[str, dict[str, Any]] = None,
        mq_manager_parameters: MQManagerConnectionParameters | None = None,
        task_runner: TaskRunnerProtocol | typing.Callable[[Job], Any] = None,
        runner_cls: typing.Type[RunnerProtocol],
    ):

        self._worker_id = str(uuid.uuid4())
        self._connection_parameters = connection_parameters

        self._client = AsyncIOMotorClient(connection_parameters.mongo_uri)
        self._q = self._client[connection_parameters.db_name][self.collection]

        self._mq_manager_parameters = mq_manager_parameters
        self.channel = channel

        self._max_concurrency: int = 1
        self._dequeuing_delay: int = 3
        self._nb_process: int = 1
        self._process_pool = multiprocessing.Pool(processes=self._nb_process)

        self._all_events = all_events
        self._tasks = set()

        # customizing classes
        self._runner_cls = runner_cls
        self._task_runner = task_runner

    def connect(self):
        MQManager.register("events_by_job_id")
        MQManager.register("init_cancel_event_for_worker_id")

        authkey = self._mq_manager_parameters.authkey
        m = MQManager(
            address=(self._mq_manager_parameters.url, self._mq_manager_parameters.port),
            authkey=authkey,
        )
        m.connect()
        self._all_events = m.events_by_job_id()
        # noinspection PyUnresolvedReferences
        m.init_cancel_event_for_worker_id(self._worker_id)
        return m

    @property
    def stop_process_event(self):
        return self._all_events.get("cancel_event_by_job_id").get(self._worker_id)

    def init_cancel_event(self, event):
        """
        init cancel event in local environment i.e. workers
        are direct children of the main process
        Args:
            event:

        Returns:

        """
        # init_cancel_event_for_worker_id
        self._all_events["cancel_event_by_job_id"][self._worker_id] = event

    def with_task_runner(
        self, task_runner: TaskRunnerProtocol | typing.Callable[[Job], Any]
    ):
        if self._task_runner is not None:
            raise ValueError(
                f"task runner already set to {self._task_runner}."
                f" May be inherited from registered_task runner."
                f" Please check your task runner registration."
            )
        self._task_runner = task_runner
        return self

    async def start(self):
        await self._q.insert_one(
            dict(
                worker_id=self._worker_id,
                running=WorkerStatus.RUNNING,
                nb_tasks=0,
                started_at=datetime.datetime.utcnow(),
                ended_at=None,
            )
        )

        connection_parameters = self._connection_parameters
        self._process_pool.apply_async(
            partial(
                syncify,
                partial(
                    build_runner,
                    self._runner_cls,
                    self._task_runner,
                    self._worker_id,
                    connection_parameters.mongo_uri,
                    connection_parameters.db_name,
                    connection_parameters.collection,
                    self._all_events,
                    self.stop_process_event,
                ),
            )
        )
        # closing directly
        self._process_pool.close()

        # join in a thread
        asyncio.get_running_loop().run_in_executor(None, self._process_pool.join)

        def cancel_task():
            cancel = asyncio.ensure_future(self.terminate())
            cancel.add_done_callback(self._tasks.discard)
            self._tasks.add(cancel)

        asyncio.get_event_loop().add_signal_handler(signal.SIGINT, cancel_task)

        return self

    async def terminate(self):
        self.stop_process_event.set()
        await wait_for_event_cleared(self.stop_process_event)
        await self._q.find_one_and_update(
            dict(worker_id=self._worker_id),
            {
                "$set": {
                    "running": WorkerStatus.TERMINATED,
                    "ended_at": datetime.datetime.utcnow(),
                }
            },
        )
        self._client.close()
        self._process_pool.terminate()

    async def scale_up(self, up: int):
        await self.terminate()
        self._nb_process = up
        logger.info("scaling worker {} to {} processes", self._worker_id, up)
        self._process_pool = multiprocessing.Pool(processes=self._nb_process)
