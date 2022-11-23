import asyncio
import datetime
import multiprocessing
import signal
from enum import IntEnum
from functools import partial

from loguru import logger

# noinspection PyProtectedMember
from motor.motor_asyncio import AsyncIOMotorDatabase

from mq._runner import RunnerProtocol
from mq.utils import wait_for_event_cleared


class NoDaemonProcess(multiprocessing.Process):
    # make 'daemon' attribute always return False
    @property
    def daemon(self):
        return False

    @daemon.setter
    def daemon(self, val):
        pass


class NoDaemonProcessPool(multiprocessing.pool.Pool):
    def Process(self, *args, **kwds):
        proc = super(NoDaemonProcessPool, self).Process(*args, **kwds)
        proc.__class__ = NoDaemonProcess

        return proc


def syncify(coroutine_function):
    """
    Args:
        coroutine_function:
        *args:
        **kwargs:
    Returns:
    """
    try:
        asyncio.run(coroutine_function())
    except KeyboardInterrupt:
        logger.debug("Keyboard interrupted")
    logger.debug("Finished")


def build_runner(
    runner: RunnerProtocol,
):
    return runner.dequeue()


class WorkerStatus(IntEnum):
    TERMINATED = 0
    RUNNING = 1


class Worker:
    collection = "mq_workers"

    def __init__(
        self,
        *,
        worker_id: str,
        db: AsyncIOMotorDatabase,
        nb_processes: int = 1,
        runner: RunnerProtocol,
        worker_stop_event: multiprocessing.Event,
        parent_manager,
    ):
        self._q = db[self.collection]
        self.worker_id = worker_id
        self._runner = runner
        self._nb_process = nb_processes
        self.worker_stop_event = worker_stop_event
        self.parent_manager = parent_manager
        self._process_executor: multiprocessing.Pool = NoDaemonProcessPool(
            processes=nb_processes,
        )
        self._tasks = set()
        self.future: asyncio.Future | None = None

    async def start(self):
        self._q.insert_one(
            dict(
                worker_id=self.worker_id,
                running=WorkerStatus.RUNNING,
                nb_tasks=0,
                started_at=datetime.datetime.utcnow(),
                ended_at=None,
            )
        )

        self._process_executor.apply_async(
            syncify,
            args=(partial(build_runner, self._runner),),
            error_callback=logger.debug,
        )
        # closing directly
        self._process_executor.close()

        # join in a thread
        loop = asyncio.get_running_loop()
        loop.run_in_executor(None, self._process_executor.join)

        def cancel_task():
            cancel = asyncio.ensure_future(self.terminate())
            cancel.add_done_callback(self._tasks.discard)
            self._tasks.add(cancel)

        loop.add_signal_handler(signal.SIGINT, cancel_task)

        return self

    async def terminate(self):
        self.worker_stop_event.set()
        await wait_for_event_cleared(self.worker_stop_event, 10)
        await self._q.find_one_and_update(
            dict(worker_id=self.worker_id),
            {
                "$set": {
                    "running": WorkerStatus.TERMINATED,
                    "ended_at": datetime.datetime.utcnow(),
                }
            },
        )
        self.parent_manager.shutdown()
        self._process_executor.terminate()

    async def scale_up(self, up: int, max_concurrency: int):
        await self.terminate()
        self._nb_process = up
        self._max_concurrency = max_concurrency
        logger.info("scaling worker {} to {} processes", self.worker_id, up)
        self._process_executor = multiprocessing.Pool(processes=self._nb_process)
