import argparse
import glob
import os
import logging

from bio_autorun.datasets.generic import Dataset
from bio_autorun.job import Job
from bio_autorun.task import Task

logger = logging.getLogger(__name__)


def build_tree_search_parser(parser: argparse.ArgumentParser):
    parser.add_argument("--rerun_incomplete", action="store_true", default=True,
                        help="If set, rerun incomplete jobs. True by default.")


class MPBootTreeSearch(Task):
    def __init__(self, *, commands: dict[str, str], dataset: Dataset, output: str, seeds: list[int], **kwargs):
        super().__init__(**kwargs)
        self.commands = commands
        self.dataset = dataset
        self.output = output
        self.seeds = seeds

    @Task.register("tree_search", build=build_tree_search_parser)
    def run_tree_search(self, *args, rerun_incomplete: bool, **kwargs):
        with self.executor.acquire():
            if not os.path.exists(self.output):
                logger.info(f"Creating output directory: {self.output}")
                os.makedirs(self.output)
            for msa in self.dataset:
                for command_name, command in self.commands.items():
                    for seed in self.seeds:
                        job_name = f"{msa.name}_{command_name}_{seed}"
                        prefix = f"{self.output}/{job_name}"
                        if os.path.exists(prefix + ".mpboot"):
                            if os.path.exists(prefix + ".log"):
                                # The log might have been overwritten
                                with open(prefix + ".log", "r") as log_file:
                                    log_content = log_file.read()
                                    if "Analysis results written to: " in log_content:
                                        logger.info(f"Job {job_name} already completed. Skipping.")
                                        continue
                        if os.path.exists(prefix + ".log"):
                            if rerun_incomplete:
                                logger.info(f"Log file for {job_name} already exists. Rerunning.")
                                for file in glob.glob(prefix + ".*"):
                                    os.remove(file)
                            else:
                                logger.warning(f"Log file for {job_name} already exists. Skipping.")
                        self.executor.submit(Job(
                            name=job_name,
                            cmd=command+[
                                "-s", msa.path,
                                "-pre", prefix,
                                "-seed", str(seed)
                            ]
                        ))

    @Task.register("analyze")
    def run_analysis(self, *args, **kwargs):
        raise NotImplementedError("Task analysis is not implemented yet.")
