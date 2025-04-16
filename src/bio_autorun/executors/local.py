from concurrent.futures.thread import ThreadPoolExecutor
from datetime import datetime
import subprocess
from typing import Optional, override

from bio_autorun.executors.generic import Executor, ExecutorConfig
from bio_autorun.job import Job, JobStatus


class LocalJob(Job):
    def __init__(self, *, pid: Optional[int] = None, **kwargs):
        super().__init__(**kwargs)
        self.pid = pid


class LocalExecutorConfig(ExecutorConfig):
    def __init__(self, *, max_workers: int, **kwargs):
        super().__init__(**kwargs)
        self.max_workers = max_workers


class LocalExecutor(Executor):
    def __init__(self, config: LocalExecutorConfig):
        super().__init__(config)
        self._pool = ThreadPoolExecutor(max_workers=config.max_workers)

    @override
    def enter_loop(self):
        super().enter_loop()
        self._pool.__enter__()

    @override
    def exit_loop(self, exc_type=None, exc_value=None, traceback=None):
        self._pool.__exit__(exc_type, exc_value, traceback)
        return super().exit_loop(exc_type, exc_value, traceback)

    def _run_job(self, job: LocalJob):
        proc = subprocess.Popen([job.cmd] + job.args, cwd=job.cwd, env=job.env, start_new_session=True)
        job.pid = proc.pid
        self.event_publish(JobStatus.RUNNING, job)

        # wait for the job to finish
        proc.wait()

        job.finished_time = datetime.now()
        job.status = JobStatus.COMPLETED
        job.exit_code = proc.returncode
        self.event_publish(JobStatus.COMPLETED, job)

    def submit(self, job: Job):
        job = LocalJob(name=job.name, cmd=job.cmd, args=job.args, cwd=job.cwd, env=job.env)
        job.submitted_time = datetime.now()
        job.status = JobStatus.SUBMITTED
        self.event_publish(JobStatus.SUBMITTED, job)

        job.queued_time = datetime.now()
        job.status = JobStatus.QUEUED
        self.event_publish(JobStatus.QUEUED, job)

        self._pool.submit(self._run_job, job)
