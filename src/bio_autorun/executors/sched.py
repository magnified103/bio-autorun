from datetime import datetime, timezone
from typing_extensions import override

import requests

from bio_autorun.executors.base import BaseExecutor, BaseExecutorConfig, ExecutorFactory
from bio_autorun.job import Job, JobStatus


class SchedulerExecutorConfig(BaseExecutorConfig):
    def __init__(self, connect_uri: str, api_key: str, **kwargs):
        super().__init__(**kwargs)
        self.connect_uri = connect_uri
        self.api_key = api_key


class SchedulerExecutor(BaseExecutor):
    config: SchedulerExecutorConfig

    def __init__(self, config: SchedulerExecutorConfig):
        super().__init__(config)

    def submit(self, job: Job):
        job = Job.from_json(job.to_json())
        job.submitted_time = datetime.now(timezone.utc)
        job.status = JobStatus.SUBMITTED

        headers = {
            "Content-Type": "application/json",
            "X-API-KEY": self.config.api_key
        }
        response = requests.post(
            f"{self.config.connect_uri}/add_job",
            json=job.to_json(),
            headers=headers,
            timeout=10
        )
        response.raise_for_status()
        self.event_publish(JobStatus.SUBMITTED, job)

    def exit_loop(self, exc_type=None, exc_value=None, traceback=None):
        response = requests.post(
            f"{self.config.connect_uri}/stop_server",
            headers={"X-API-KEY": self.config.api_key},
            timeout=10
        )
        response.raise_for_status()
        return super().exit_loop(exc_type, exc_value, traceback)
