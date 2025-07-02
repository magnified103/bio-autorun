from datetime import datetime
from typing import Optional, Union
import sys

if sys.version_info < (3, 11):
    from enum import Enum

    class JobStatus(str, Enum):
        PENDING = "pending"
        SUBMITTED = "submitted"
        QUEUED = "queued"
        STARTED = "started"
        COMPLETED = "completed"
        CANCELLED = "cancelled"

else:
    from enum import StrEnum

    class JobStatus(StrEnum):
        PENDING = "pending"
        SUBMITTED = "submitted"
        QUEUED = "queued"
        STARTED = "started"
        COMPLETED = "completed"
        CANCELLED = "cancelled"

class Job:
    def __init__(self, *, name: str, cmd: Union[str, list[str]], env: Optional[dict[str, str]] = None,
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
