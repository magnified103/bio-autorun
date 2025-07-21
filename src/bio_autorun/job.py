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
                 stdin: Optional[str] = None, stdout: Optional[str] = None, stderr: Optional[str] = None,
                 stdin_str: Optional[str] = None,
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
        self.stdin = stdin
        self.stdout = stdout
        self.stderr = stderr
        self.stdin_str = stdin_str
        self.status = status
        self.submitted_time = submitted_time
        self.queued_time = queued_time
        self.start_time = start_time
        self.end_time = end_time
        self.exit_code = exit_code

    def __str__(self):
        return self.name

    def to_json(self) -> dict:
        return {
            "name": self.name,
            "cmd": self.cmd,
            "env": self.env,
            "cwd": self.cwd,
            "shell": self.shell,
            "stdin": self.stdin,
            "stdout": self.stdout,
            "stderr": self.stderr,
            "stdin_str": self.stdin_str,
            "status": self.status.value,
            "exit_code": self.exit_code,
            "submitted_time": self.submitted_time.isoformat() if self.submitted_time else None,
            "queued_time": self.queued_time.isoformat() if self.queued_time else None,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None
        }

    @classmethod
    def from_json(cls, data: dict):
        return cls(
            name=data["name"],
            cmd=data["cmd"],
            env=data.get("env"),
            cwd=data.get("cwd"),
            shell=data.get("shell", False),
            stdin=data.get("stdin"),
            stdout=data.get("stdout"),
            stderr=data.get("stderr"),
            stdin_str=data.get("stdin_str"),
            status=data.get("status", JobStatus.PENDING),
            exit_code=data.get("exit_code"),
            submitted_time=datetime.fromisoformat(data["submitted_time"]) if data.get("submitted_time") else None,
            queued_time=datetime.fromisoformat(data["queued_time"]) if data.get("queued_time") else None,
            start_time=datetime.fromisoformat(data["start_time"]) if data.get("start_time") else None,
            end_time=datetime.fromisoformat(data["end_time"]) if data.get("end_time") else None
        )
