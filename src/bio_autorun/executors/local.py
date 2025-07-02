import concurrent.futures
from concurrent.futures.thread import ThreadPoolExecutor
from datetime import datetime
import logging
import subprocess
from typing_extensions import Optional, override

from bio_autorun.executors.base import BaseExecutor, BaseExecutorConfig, ExecutorFactory
from bio_autorun.job import Job, JobStatus

logger = logging.getLogger(__name__)


class LocalJob(Job):
    def __init__(self, *, pid: Optional[int] = None, **kwargs):
        super().__init__(**kwargs)
        self.pid = pid


class LocalExecutorConfig(BaseExecutorConfig):
    def __init__(self, *, max_workers: int, **kwargs):
        super().__init__(**kwargs)
        self.max_workers = max_workers


class LocalExecutor(BaseExecutor):
    def __init__(self, config: LocalExecutorConfig):
        super().__init__(config)
        self._pool = ThreadPoolExecutor(max_workers=config.max_workers)
        self._futures = []

    @override
    def enter_loop(self):
        super().enter_loop()
        self._pool.__enter__()

    @override
    def exit_loop(self, exc_type=None, exc_value=None, traceback=None):
        done_tasks, not_done_tasks = concurrent.futures.wait(
            self._futures, return_when=concurrent.futures.FIRST_EXCEPTION
        )
        for task in done_tasks:
            err = task.exception()
            if err is not None:
                logger.error(err)
        self._pool.__exit__(exc_type, exc_value, traceback)
        return super().exit_loop(exc_type, exc_value, traceback)

    def _run_job(self, job: LocalJob):
        proc = subprocess.Popen(
            job.cmd,
            cwd=job.cwd,
            env=job.env,
            shell=job.shell,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        job.pid = proc.pid
        job.start_time = datetime.now()
        self.event_publish(JobStatus.STARTED, job)

        # wait for the job to finish
        proc.wait()

        job.end_time = datetime.now()
        job.status = JobStatus.COMPLETED
        job.exit_code = proc.returncode
        self.event_publish(JobStatus.COMPLETED, job)

    def submit(self, job: Job):
        job = LocalJob(
            name=job.name, cmd=job.cmd, cwd=job.cwd, env=job.env, shell=job.shell
        )
        job.submitted_time = datetime.now()
        job.status = JobStatus.SUBMITTED
        self.event_publish(JobStatus.SUBMITTED, job)

        job.queued_time = datetime.now()
        job.status = JobStatus.QUEUED
        self.event_publish(JobStatus.QUEUED, job)

        self._futures.append(self._pool.submit(self._run_job, job))


ExecutorFactory.register(LocalExecutorConfig, LocalExecutor)
