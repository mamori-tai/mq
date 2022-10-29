import datetime
from typing import Protocol

# noinspection PyProtectedMember
from motor.motor_asyncio import AsyncIOMotorCollection
from schedule import Job as ScheduleJob

from mq._job import JobStatus
from mq.utils import loads, dumps


class SchedulerProtocol(Protocol):
    q: AsyncIOMotorCollection

    async def schedule(self, *args, **kwargs):
        pass

    def mongo_query(self):
        pass


class DefaultScheduler:
    def __init__(self, collection: AsyncIOMotorCollection):
        self.q = collection

    async def schedule(self, job_id: str, schedule_job, status: JobStatus, result=None):
        now = datetime.datetime.now()
        is_job_scheduled = schedule_job is not None
        schedule_job: ScheduleJob = loads(schedule_job) if is_job_scheduled else None

        if is_job_scheduled:
            schedule_job.last_run = now
            # noinspection PyProtectedMember
            schedule_job._schedule_next_run()

        computed_status = status
        if status != JobStatus.CANCELLED:
            computed_status = status if not is_job_scheduled else JobStatus.WAITING

        await self.q.find_one_and_update(
            {"_id": job_id},
            {
                "$set": {
                    "status": computed_status,
                    "last_run_at": now,
                    "next_run_at": schedule_job.next_run if is_job_scheduled else None,
                    "schedule": dumps(schedule_job) if is_job_scheduled else None,
                    "result": result,
                }
            },
        )

    def mongo_query(self):
        return {
            "next_run_at": {"$lte": datetime.datetime.now()},
            "status": JobStatus.WAITING,
        }
