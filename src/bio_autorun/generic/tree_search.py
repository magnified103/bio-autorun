import logging
import os

from bio_autorun.job import Job
from bio_autorun.task import Task


logger = logging.getLogger(__name__)


class GenericTreeSearchBase(Task):
    def __init__(self, *args, commands, dataset, output, seeds, stdin_str=None, stdin=None,
                 stdout=None, stderr=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.commands = commands
        self.dataset = dataset
        self.output = output
        self.seeds = seeds
        self.stdin_str = stdin_str
        self.stdin = stdin
        self.stdout = stdout
        self.stderr = stderr


class GenericTreeSearch(GenericTreeSearchBase):
    def __call__(self, *args, **kwargs):
        with self.executor.acquire():
            if not os.path.exists(self.output):
                logger.info(f"Creating output directory: {self.output}")
                os.makedirs(self.output)
            for msa in self.dataset:
                for command_name, command in self.commands.items():
                    for seed in self.seeds:
                        job_name = f"{msa.name}_{command_name}_{seed}"
                        prefix = f"{self.output}/{job_name}"

                        # build context
                        context = {
                            "name": job_name,
                            "msa": msa.path,
                            "seed": seed,
                            "prefix": prefix,
                            "msa_type": "prot" if msa.category == "protein" else msa.category,
                        }

                        # build arguments
                        assert isinstance(command, list)
                        parsed_command = []
                        for arg in command:
                            parsed_command.append(arg.format(**context))

                        self.executor.submit(Job(
                            name=job_name,
                            cmd=parsed_command,
                            stdin=self.stdin.format(**context) if self.stdin else None,
                            stdout=self.stdout.format(**context) if self.stdout else None,
                            stderr=self.stderr.format(**context) if self.stderr else None,
                            stdin_str=self.stdin_str.format(**context) if self.stdin_str else None,
                        ))
