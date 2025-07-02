from datetime import datetime
import logging
import os
from typing_extensions import override

from bio_autorun.executors.base import BaseExecutor, BaseExecutorConfig, ExecutorFactory
from bio_autorun.job import Job, JobStatus

logger = logging.getLogger(__name__)


class SlurmJob(Job):
    pass


class BaseSlurmExecutorConfig(BaseExecutorConfig):
    def __init__(
        self,
        batch_name,
        batch_script_path: str,
        cmd_list_path: str,
        hold: bool = False,
        **kwargs,
    ):
        self.batch_name = batch_name
        self.batch_script_path = batch_script_path
        self.cmd_list_path = cmd_list_path
        self.hold = hold


class BaseSlurmExecutor(BaseExecutor):
    config: BaseSlurmExecutorConfig

    def __init__(self, config: BaseSlurmExecutorConfig):
        super().__init__(config)
        self.cmd_list: list[str] = None

    @override
    def enter_loop(self):
        super().enter_loop()
        self.cmd_list = []

    def submit(self, job: Job):
        job = SlurmJob(
            name=job.name, cmd=job.cmd, cwd=job.cwd, env=job.env, shell=job.shell
        )
        if isinstance(job.cmd, str):
            self.cmd_list.append(job.cmd)
        else:
            self.cmd_list.append("'" + "' '".join(job.cmd) + "'")
        job.submitted_time = datetime.now()
        job.status = JobStatus.SUBMITTED
        self.event_publish(JobStatus.SUBMITTED, job)


class SlurmExecutorConfig(BaseSlurmExecutorConfig):
    pass


class SlurmExecutor(BaseSlurmExecutor):
    @override
    def exit_loop(self, exc_type=None, exc_value=None, traceback=None):
        if exc_type is None:
            with open(self.config.cmd_list_path, "x") as f:
                for cmd in self.cmd_list:
                    f.write(f"{cmd}\n")

            with open(self.config.batch_script_path, "x") as f:
                f.write("#!/bin/bash\n")
                if self.config.hold:
                    f.write(f"#SBATCH --hold\n")
                f.write(f"#SBATCH --job-name={self.config.batch_name}\n")
                f.write(f"#SBATCH --ntasks=1\n")
                f.write(f"#SBATCH --cpus-per-task=1\n")
                f.write(f"#SBATCH --array=1-{len(self.cmd_list)}\n")
                f.write(f"#SBATCH --output=slurm-log/slurm-%A_%a.out\n")
                f.write(
                    f'command=$(sed "${{SLURM_ARRAY_TASK_ID}}q;d" {self.config.cmd_list_path})\n'
                )
                f.write("eval $command\n")
        return super().exit_loop(exc_type, exc_value, traceback)


class PreallocSlurmExecutorConfig(BaseSlurmExecutorConfig):
    def __init__(self, srun_runner_script: str = None, **kwargs):
        super().__init__(**kwargs)
        self.srun_runner_script = srun_runner_script


class PreallocSlurmExecutor(BaseSlurmExecutor):
    @override
    def exit_loop(self, exc_type=None, exc_value=None, traceback=None):
        if exc_type is None:
            # The list of commands is written to a file
            with open(self.config.cmd_list_path, "x") as f:
                for cmd in self.cmd_list:
                    f.write(f"{cmd}\n")

            # The runner script is created to run multiple commands in order
            with open(self.config.srun_runner_script, "x") as f:
                f.write("#!/bin/bash\n")
                f.write('readarray -t commands < "$1"\n')
                f.write("start=${2:-0}\n")
                f.write("end=${3:-${#commands[@]}}\n")
                f.write(
                    "for (( i=start+$SLURM_PROCID; i<end; i+=$SLURM_NTASKS )); do\n"
                )
                f.write("    eval ${commands[i]} < /dev/null\n")
                f.write("done\n")
            os.chmod(self.config.srun_runner_script, 0o700)

            # The batch script is created to run the runner script in parallel
            with open(self.config.batch_script_path, "x") as f:
                f.write("#!/bin/bash\n")
                if self.config.hold:
                    f.write(f"#SBATCH --hold\n")
                assert os.path.exists(self.config.srun_runner_script)
                f.write(f"#SBATCH --job-name={self.config.batch_name}\n")
                f.write(f"#SBATCH --output=slurm-log/slurm-%A.out\n")
                f.write(
                    f'srun {self.config.srun_runner_script} {self.config.cmd_list_path} "$@"\n'
                )
        return super().exit_loop(exc_type, exc_value, traceback)


ExecutorFactory.register(PreallocSlurmExecutorConfig, PreallocSlurmExecutor)
ExecutorFactory.register(SlurmExecutorConfig, SlurmExecutor)
