from typing import Protocol

from motor.motor_asyncio import AsyncIOMotorCollection


class Scheduler(Protocol):
    q: AsyncIOMotorCollection

    async def save_last_execution(self, job):
        pass

    async def compute_next_execution(self, job):
        pass

    def mongo_query(self):
        pass


class DefaultScheduler:
    pass
