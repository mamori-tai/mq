import datetime
import enum
import pickle
from typing import Any

import bson
from motor.motor_asyncio import AsyncIOMotorCollection
from pydantic import BaseModel, Field


class JobStatus(str, enum.Enum):
    CANCELLED = "cancelled"
    WAITING = "waiting"
    PENDING = "pending"
    ON_ERROR = "on_error"
    FINISHED = "finished"


class Job(BaseModel):
    class Config:
        allow_population_by_field_name = True
        arbitrary_types_allowed = True

    id: str = Field("", alias="_id")
    f: bson.Binary

    status: JobStatus = JobStatus.WAITING

    enqueued_at: datetime.datetime = datetime.datetime.now()
    started_at: datetime.datetime | None = None
    ended_at: datetime.datetime | None = None

    result: bson.Binary | None = None

    async def enqueue(self, q: AsyncIOMotorCollection):
        await q.insert_one(self.dict(by_alias=True))

    async def _update(self, update: dict[str, Any], q: AsyncIOMotorCollection) -> None:
        await q.find_one_and_update({"_id": self.id}, {"$set": update})

    async def _update_with_job_status_end_date_key(
        self, status: JobStatus, date_key: str, q: AsyncIOMotorCollection
    ) -> None:
        await self._update({"status": status, date_key: datetime.datetime.now()}, q)

    async def pending(self, q: AsyncIOMotorCollection):
        await self._update_with_job_status_end_date_key(
            JobStatus.PENDING, "started_at", q
        )

    async def finished(self, result: Any, q: AsyncIOMotorCollection):
        await self._update(
            {
                "status": JobStatus.FINISHED,
                "ended_at": datetime.datetime.now(),
                "result": pickle.dumps(result),
            },
            q,
        )

    async def on_error(self, q: AsyncIOMotorCollection):
        await self._update_with_job_status_end_date_key(
            JobStatus.ON_ERROR, "ended_at", q
        )

    async def refresh(self, q):
        r = await q.find_one({"_id": self.id})
        return Job(**r)
