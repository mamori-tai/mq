import datetime
from typing import Any, Protocol

# noinspection PyProtectedMember
from schedule import Job as ScheduleJob

from mq._job import Job, JobStatus
from mq.utils import dumps, loads


class SchedulerProtocol(Protocol):
    def on_enqueue_job(self, current_job: Job, schedule_policy: Any = None, retry_policy: Any = None):
        pass

    def schedule_job(self, current_job: Job) -> bool:
        ...

    def mongo_query(self) -> dict[str, Any]:
        pass


class DefaultScheduler:
    def on_enqueue_job(self, current_job: Job, schedule_policy: ScheduleJob = None, retry_policy: Any = None):
        current_job.extra["retry"] = dumps(retry_policy)

        if schedule_policy is None:
            current_job.next_run_at = datetime.datetime.now()
            return
        schedule_policy._schedule_next_run()
        current_job.next_run_at = schedule_policy.next_run
        current_job.extra["schedule"] = dumps(schedule_policy)

    def schedule_job(self, current_job: Job):
        try:
            now = datetime.datetime.now()
            current_job.last_run_at = now

            if (schedule_obj_bin := current_job.extra.get("schedule")) is None:
                current_job.next_run_at = None
                return False

            schedule_obj = loads(schedule_obj_bin)
            # noinspection PyProtectedMember
            schedule_obj._schedule_next_run()

            current_job.next_run_at = schedule_obj.next_run
            schedule_obj.last_run = now
            current_job.extra["schedule"] = dumps(schedule_obj)
            return True
        except Exception:
            raise

    def mongo_query(self):
        return {
            "next_run_at": {"$lte": datetime.datetime.now()},
            "status": JobStatus.WAITING,
        }
