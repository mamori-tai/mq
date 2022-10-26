import asyncio
import multiprocessing
import typing
import uuid
from functools import partial
from multiprocessing.managers import SyncManager
from typing import TYPE_CHECKING, Any

from loguru import logger

from mq._job import Job
from mq._runner import Runner, TaskRunnerProtocol
from mq.utils import _cancel_all_tasks, wait_for_event_cleared, MongoDBConnectionParameters, \
    MQManagerConnectionParameters

if TYPE_CHECKING:
    from mq.mq import MQ


def syncify(coroutine_function, *args, **kwargs):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    def reraise(task_):
        if task_.cancelled():
            task_.exception()
        exc = task_.exception()
        if exc:
            raise exc

    try:
        task = loop.create_task(coroutine_function(*args, **kwargs))
        task.add_done_callback(reraise)
        loop.run_until_complete(task)
        _cancel_all_tasks(loop)
    except Exception as e:
        logger.exception(e)
        raise e


def build_runner(task_runner_cls: typing.Type[TaskRunnerProtocol], *args, **kwargs):
    runner = Runner(task_runner_cls, *args, **kwargs)
    return runner.dequeue()


class MQManager(SyncManager):
    pass


class Worker:
    def __init__(self, connection_parameters: MongoDBConnectionParameters, channel: str, all_events: dict[str, dict[str, Any]] = None, mq_manager_parameters: MQManagerConnectionParameters | None = None, task_runner_cls: typing.Type[TaskRunnerProtocol] = None):
        self._worker_id = str(uuid.uuid4())
        self._connection_parameters = connection_parameters
        self._mq_manager_parameters = mq_manager_parameters
        self._channel = channel
        self._max_concurrency: int = 1
        self._dequeuing_delay: int = 3
        self._nb_process: int = 1
        self._process_pool = multiprocessing.Pool(processes=self._nb_process)
        self._all_events = all_events
        self._task_runner_cls = task_runner_cls

        # connect if server parameters configured
        if mq_manager_parameters is not None:
            self.connect()

    def connect(self):
        MQManager.register("events_by_job_id")
        MQManager.register("init_cancel_event_for_worker_id")

        authkey = self._mq_manager_parameters.authkey
        m = MQManager(
            address=(self._mq_manager_parameters.url, self._mq_manager_parameters.port),
            authkey=authkey
        )
        m.connect()
        self._all_events = m.events_by_job_id()
        # noinspection PyUnresolvedReferences
        m.init_cancel_event_for_worker_id(self._worker_id)
        logger.debug("{}, {}",self._worker_id, self._all_events)
        return m

    @property
    def stop_process_event(self):
        return self._all_events.get("cancel_event_by_job_id").get(self._worker_id)

    def _init_cancel_event(self, event):
        """

        Args:
            event:

        Returns:

        """
        # init_cancel_event_for_worker_id
        self._all_events["cancel_event_by_job_id"][self._worker_id] = event


    def with_task_runner(self, task_runner_cls: TaskRunnerProtocol | typing.Callable[[Job], Any]):
        self._task_runner_cls = task_runner_cls
        return self

    def start(self):
        connection_parameters = self._connection_parameters
        self._process_pool.apply_async(
            partial(
                syncify,
                partial(
                    build_runner,
                    self._task_runner_cls,
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
        # start a dummy thread to join
        asyncio.get_event_loop().run_in_executor(None, self._process_pool.join)
        return self

    async def terminate(self):
        self.stop_process_event.set()
        await wait_for_event_cleared(self.stop_process_event)
        self._process_pool.terminate()


    async def scale_up(self, up: int):
        await self.terminate()
        self._nb_process = up
        logger.info("scaling worker {} to {} processes", self._worker_id, up)
        self._process_pool = multiprocessing.Pool(processes=self._nb_process)
        self.start()


#class register_task_runner:
#    def __call__(self, fn):
#        @functools.wraps(fn)
#        async def _(current_jobs):
#
#            return await mq.job_queue.enqueue(fn, *args, **kwargs)
#
#        fn.task_runner = _
