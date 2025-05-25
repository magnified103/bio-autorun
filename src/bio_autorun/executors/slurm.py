from datetime import datetime
from typing import override
import logging
from bio_autorun.executors.generic import Executor, ExecutorConfig
from bio_autorun.job import Job, JobStatus

logger = logging.getLogger(__name__)


class SlurmJob(Job):
    pass


class SlurmExecutorConfig(ExecutorConfig):
    def __init__(self, batch_name, batch_script_path: str, cmd_list_path: str, hold: bool = False):
        self.batch_name = batch_name
        self.batch_script_path = batch_script_path
        self.cmd_list_path = cmd_list_path
        self.hold = hold


class SlurmExecutor(Executor):
    def __init__(self, config: SlurmExecutorConfig):
        super().__init__(config)
        self.cmd_list: list[str] = None
    
    @override
    def enter_loop(self):
        super().enter_loop()
        self.cmd_list = []
    
    @override
    def exit_loop(self, exc_type=None, exc_value=None, traceback=None):
        if exc_type is None:
            with open(self.config.cmd_list_path, 'w') as f:
                for cmd in self.cmd_list:
                    f.write(f"{cmd}\n")
            with open(self.config.batch_script_path, 'w') as f:
                f.write("#!/bin/bash\n")
                f.write(f"#SBATCH --hold\n")
                f.write(f"#SBATCH --job-name={self.config.batch_name}\n")
                f.write(f"#SBATCH --ntasks=1\n")
                f.write(f"#SBATCH --cpus-per-task=1\n")
                f.write(f"#SBATCH --array=1-{len(self.cmd_list)}\n")
                f.write(f"#SBATCH --output=slurm-log/slurm-%A_%a.out\n")
                f.write(f"command=$(sed \"${{SLURM_ARRAY_TASK_ID}}q;d\" {self.config.cmd_list_path})\n")
                f.write("eval $command\n")
        return super().exit_loop(exc_type, exc_value, traceback)
    
    def submit(self, job):
        if isinstance(job.cmd, str):
            self.cmd_list.append(job.cmd)
        else:
            self.cmd_list.append(" ".join(job.cmd))
        job.submitted_time = datetime.now()
        job.status = JobStatus.SUBMITTED
        self.event_publish(JobStatus.SUBMITTED, job)
