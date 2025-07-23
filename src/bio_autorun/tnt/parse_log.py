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


class TNTParseLog(Task):
    def __init__(self, *args, command_names: list[str], dataset: Union[Dataset, Iterable[MSA]], output: str,
                 seeds: list[int], **kwargs):
        super().__init__(*args, **kwargs)
        self.command_names = command_names
        self.dataset = dataset
        self.output = output
        self.seeds = seeds
        self.parser.add_argument("--analysis-output", type=str, required=True,
                        help="Directory to save analysis results.")

    def __call__(self, *, analysis_output: str, **kwargs):
        # list of raw scores and times for each command and MSA
        scores_per_command: dict[str, dict[str, list[int]]] = {}
        times_per_command: dict[str, dict[str, list[int]]] = {}

        for msa in self.dataset:
            logger.info(f"Parsing logs for MSA {msa.name}")
            for command_name in self.command_names:
                scores = scores_per_command.setdefault(command_name, {})
                times = times_per_command.setdefault(command_name, {})

                score_list = []
                time_list = []
                try:
                    for seed in self.seeds:
                        job_name = f"{msa.name}_{command_name}_{seed}"
                        prefix = f"{self.output}/{job_name}"
                        if not os.path.exists(prefix + ".boottrees") or not os.path.exists(prefix + ".log"):
                            raise RuntimeError(f"Missing output files for job {job_name}.")
                        with open(prefix + ".log", "r") as log_file:
                            log_content = log_file.read()
                        score_match = re.search(r"^Best score: (-?\d+)\.", log_content, re.M)
                        best_score = int(score_match.group(1)) if score_match else None
                        xmult_time_match = re.search(r"^xmult (\d+\.\d+) secs\.", log_content, re.M)
                        sample_time_match = re.search(r"^(\d+\.\d+) secs\. to complete resampling", log_content, re.M)
                        xmult_time = float(xmult_time_match.group(1)) if xmult_time_match else None
                        sample_time = float(sample_time_match.group(1)) if sample_time_match else None
                        
                        if best_score is None:
                            raise RuntimeError(f"Could not find best score for job {job_name}")
                        if xmult_time is None:
                            raise RuntimeError(f"Could not find xmult time for job {job_name}")
                        if sample_time is None:
                            raise RuntimeError(f"Could not find sample time for job {job_name}")
                        cpu_time = xmult_time + sample_time
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

        for command_name in self.command_names:
            scores_df = pd.DataFrame(scores_per_command[command_name].items(), columns=["MSA", "Scores"])
            times_df = pd.DataFrame(times_per_command[command_name].items(), columns=["MSA", "Runtimes"])
            df = pd.merge(scores_df, times_df, on="MSA")
            df.to_csv(f"{analysis_output}/{command_name}.csv", index=False)
