import dataclasses
import datetime
import enum
from typing import Any

import bson


class JobStatus(str, enum.Enum):
    CANCELLED = "cancelled"
    WAITING = "waiting"
    PENDING = "pending"
    ON_ERROR = "on_error"
    FINISHED = "finished"


@dataclasses.dataclass
class Job:
    _id: str
    f: bson.Binary | None = None
    payload: dict[str, Any] | None = None

    locked_by: str | None = None
    status: JobStatus = dataclasses.field(default=JobStatus.WAITING)

    enqueued_at: datetime.datetime = dataclasses.field(
        default_factory=datetime.datetime.now
    )
    last_duration: int | None = None
    last_run_at: datetime.datetime | None = None
    next_run_at: datetime.datetime | None = None
    result: bson.Binary | None = None

    extra: dict[str, Any] = dataclasses.field(default_factory=dict)

    @property
    def id(self):
        return self._id
