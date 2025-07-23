import glob
import os
import logging
import re
from typing import Iterable, Union

import pandas as pd

from bio_autorun.datasets.generic import Dataset
from bio_autorun.job import Job
from bio_autorun.msa import MSA
from bio_autorun.task import Task

logger = logging.getLogger(__name__)


class MPBootTreeSearchBase(Task):
    def __init__(self, name: str, *, commands: dict[str, str], dataset: Union[Dataset, Iterable[MSA]], output: str,
                 seeds: list[int], skipped_jobs: list[str] = [], **kwargs):
        super().__init__(name, **kwargs)
        self.commands = commands
        self.dataset = dataset
        self.output = output
        self.seeds = seeds
        self.skipped_jobs = skipped_jobs


class MPBootTreeSearch(MPBootTreeSearchBase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.parser.add_argument("--rerun-incomplete", action="store_true", default=False,
                        help="If set, rerun incomplete jobs. False by default.")
        self.parser.add_argument("--overwrite-check", action="store_true", default=False,
                        help="If set, perform the log overwrite check. False by default.")

    def __call__(self, *args, rerun_incomplete: bool, overwrite_check: bool, **kwargs):
        with self.executor.acquire():
            if not os.path.exists(self.output):
                logger.info(f"Creating output directory: {self.output}")
                os.makedirs(self.output)
            for msa in self.dataset:
                for command_name, command in self.commands.items():
                    for seed in self.seeds:
                        job_name = f"{msa.name}_{command_name}_{seed}"
                        if job_name in self.skipped_jobs:
                            logger.debug(f"Skipping job {job_name} as it is in the skipped jobs list.")
                            continue
                        prefix = f"{self.output}/{job_name}"
                        if os.path.exists(prefix + ".mpboot"):
                            if overwrite_check and os.path.exists(prefix + ".log"):
                                # The log might have been overwritten
                                with open(prefix + ".log", "r") as log_file:
                                    log_content = log_file.read()
                                    if "Analysis results written to: " in log_content:
                                        logger.debug(f"Job {job_name} already completed. Skipping.")
                                        continue
                            else:
                                logger.debug(f"Job {job_name} already completed. Skipping.")
                                continue
                        if os.path.exists(prefix + ".log"):
                            if rerun_incomplete:
                                logger.info(f"Log file for {job_name} already exists, but mpboot output is missing. Rerunning job.")
                                for file in glob.glob(prefix + ".*"):
                                    os.remove(file)
                            else:
                                logger.warning(f"Log file for {job_name} already exists. Skipping.")
                                continue
                        self.executor.submit(Job(
                            name=job_name,
                            cmd=command+[
                                "-s", msa.path,
                                "-pre", prefix,
                                "-seed", str(seed)
                            ]
                        ))


class MPBootParseLog(MPBootTreeSearchBase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.parser.add_argument("--analysis-output", type=str, required=True,
                        help="Directory to save analysis results.")

    def __call__(self, *, analysis_output: str, **kwargs):
        # list of raw scores and times for each command and MSA
        scores_per_command: dict[str, dict[str, list[int]]] = {}
        times_per_command: dict[str, dict[str, list[int]]] = {}

        for msa in self.dataset:
            logger.info(f"Parsing logs for MSA {msa.name}")
            for command_name, _ in self.commands.items():
                scores = scores_per_command.setdefault(command_name, {})
                times = times_per_command.setdefault(command_name, {})

                score_list = []
                time_list = []
                try:
                    for seed in self.seeds:
                        job_name = f"{msa.name}_{command_name}_{seed}"
                        prefix = f"{self.output}/{job_name}"
                        if not os.path.exists(prefix + ".mpboot") or not os.path.exists(prefix + ".log"):
                            raise RuntimeError(f"Missing output files for job {job_name}.")
                        with open(prefix + ".log", "r") as log_file:
                            log_content = log_file.read()
                            if "Analysis results written to: " not in log_content:
                                raise RuntimeError(f"Log file for job {job_name} corrupted or incomplete.")
                        score_match = re.search(r"^BEST SCORE FOUND : (-?\d+)$", log_content, re.M)
                        best_score = int(score_match.group(1)) if score_match else None
                        cpu_time_match = re.search(r"^Total CPU time used: (\d+\.\d+) sec", log_content, re.M)
                        cpu_time = float(cpu_time_match.group(1)) if cpu_time_match else None
                        if not best_score:
                            raise RuntimeError(f"Could not find best score for job {job_name}.")
                        if not cpu_time:
                            raise RuntimeError(f"Could not find CPU time for job {job_name}.")
                        cpu_time = cpu_time / 3600
                        score_list.append(best_score)
                        time_list.append(cpu_time)
                except Exception as e:
                    logger.error(f"Error parsing log for job {job_name}: {e}")
                    continue

                # verify the lengths of the lists
                assert len(score_list) == len(self.seeds)
                assert len(time_list) == len(self.seeds)

                scores[msa.name] = score_list
                times[msa.name] = time_list

        for command_name in self.commands.keys():
            scores_df = pd.DataFrame(scores_per_command[command_name].items(), columns=["MSA", "Scores"])
            times_df = pd.DataFrame(times_per_command[command_name].items(), columns=["MSA", "Runtimes"])
            df = pd.merge(scores_df, times_df, on="MSA")
            df.to_csv(f"{analysis_output}/{command_name}.csv", index=False)
