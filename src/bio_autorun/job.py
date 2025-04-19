from datetime import datetime
from enum import StrEnum
from typing import Iterable, Optional


class JobStatus(StrEnum):
    PENDING = "pending"
    SUBMITTED = "submitted"
    QUEUED = "queued"
    STARTED = "started"
    COMPLETED = "completed"
    CANCELLED = "cancelled"


class Job:
    def __init__(self, *, name: str, cmd: str | list[str], env: Optional[dict[str, str]] = None,
                 cwd: Optional[str] = None,
                 shell=False,
                 status: JobStatus = JobStatus.PENDING,
                 exit_code: Optional[int] = None,
                 submitted_time: Optional[datetime] = None, queued_time: Optional[datetime] = None,
                 start_time: Optional[datetime] = None, end_time: Optional[datetime] = None,
                 ):
        self.name = name
        self.cmd = cmd
        self.env = env
        self.cwd = cwd
        self.shell = shell
        self.status = status
        self.submitted_time = submitted_time
        self.queued_time = queued_time
        self.start_time = start_time
        self.end_time = end_time
        self.exit_code = exit_code

    def __str__(self):
        return self.name
