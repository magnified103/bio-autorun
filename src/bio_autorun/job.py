from datetime import datetime
from enum import StrEnum
from typing import Iterable, Optional


class JobStatus(StrEnum):
    PENDING = "pending"
    SUBMITTED = "submitted"
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    CANCELLED = "cancelled"


class Job:
    def __init__(self, *, name: str, cmd: str, args: Iterable[str], env: Optional[dict[str, str]] = None, cwd: Optional[str] = None,
                 status: JobStatus = JobStatus.PENDING,
                 exit_code: Optional[int] = None,
                 submitted_time: Optional[datetime] = None, queued_time: Optional[datetime] = None,
                 start_time: Optional[datetime] = None, end_time: Optional[datetime] = None,
                 ):
        self.name = name
        self.cmd = cmd
        self.args = list(args)
        self.env = env
        self.cwd = cwd
        self.status = status
        self.submitted_time = submitted_time
        self.queued_time = queued_time
        self.start_time = start_time
        self.end_time = end_time
        self.exit_code = exit_code

    def __str__(self):
        return self.name
