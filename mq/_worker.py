import asyncio
import datetime
import multiprocessing
import signal
from enum import Enum, IntEnum
from functools import partial

from loguru import logger

# noinspection PyProtectedMember
from motor.motor_asyncio import AsyncIOMotorDatabase

from mq._runner import RunnerProtocol


def syncify(coroutine_function):
    """
    Args:
        coroutine_function:
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


class WorkerType(str, Enum):
    THREAD = "thread"
    PROCESS = "process"


class Worker:
    collection = "mq_workers"

    def __init__(
        self,
        *,
        worker_id: str,
        db: AsyncIOMotorDatabase,
        nb_processes: int = 1,
        runner: RunnerProtocol,
        worker_type: WorkerType = WorkerType.THREAD,
        worker_stop_event: multiprocessing.Event,
        parent_manager,
    ):
        self._q = db[self.collection]
        self.worker_id = worker_id
        self._runner = runner
        self._nb_process = nb_processes
        self.worker_stop_event = worker_stop_event
        self.parent_manager = parent_manager
        self._worker_type = worker_type
        self._process_executor = self._pool_factory(nb_processes)
        self._tasks = set()
        self.future: asyncio.Future | None = None
        logger.debug(worker_stop_event)

    def _pool_factory(self, nb_processes: int) -> multiprocessing.pool.Pool:
        pool_inst = (
            multiprocessing.pool.ThreadPool
            if self._worker_type == WorkerType.THREAD
            else multiprocessing.Pool
        )
        return pool_inst(processes=nb_processes)

    async def start(self):
        logger.debug(
            "Starting worker {} with {} handlers", self._worker_type, self._nb_process
        )
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
        logger.debug("Worker event stopped requested {}", self.worker_stop_event)
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

        # finally joining
        self._process_executor.join()

    async def scale_up(self, up: int):  # pragma: no cover
        await self.terminate()
        self._nb_process = up
        logger.info("scaling worker {} to {} processes", self.worker_id, up)
        self._process_executor = self._pool_factory(processes=up)
