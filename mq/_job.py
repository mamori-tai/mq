import datetime
import enum
from typing import Any

import bson
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
    locked_by: str | None
    f: bson.Binary | None
    payload: dict[str, Any] | None

    status: JobStatus = JobStatus.WAITING

    enqueued_at: datetime.datetime = datetime.datetime.now()
    started_at: datetime.datetime | None = None
    ended_at: datetime.datetime | None = None

    result: bson.Binary | None = None
