import argparse
import glob
import os
import logging
import re
from typing import Iterable, Optional, Union

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

from bio_autorun.datasets.generic import Dataset
from bio_autorun.job import Job
from bio_autorun.msa import MSA
from bio_autorun.task import Task

logger = logging.getLogger(__name__)


def build_tree_search_parser(parser: argparse.ArgumentParser):
    parser.add_argument("--rerun-incomplete", action="store_true", default=False,
                        help="If set, rerun incomplete jobs. False by default.")
    parser.add_argument("--overwrite-check", action="store_true", default=False,
                        help="If set, perform the log overwrite check. False by default.")

def build_analysis_parser(parser: argparse.ArgumentParser):
    parser.add_argument("--analysis-output", type=str, required=True,
                        help="Directory to save analysis results.")
    parser.add_argument("--no-parse", action="store_true", default=False,
                        help="If set, skip parsing the logs. Useful for debugging.")
    parser.add_argument("--tnt-csv", type=str, required=False, help="Path to TNT CSV file for analysis.")

class MPBootTreeSearch(Task):
    def __init__(self, *, commands: dict[str, str], dataset: Union[Dataset, Iterable[MSA]], output: str, seeds: list[int],
                 skipped_jobs: list[str] = [], **kwargs):
        super().__init__(**kwargs)
        self.commands = commands
        self.dataset = dataset
        self.output = output
        self.seeds = seeds
        self.skipped_jobs = skipped_jobs

    @Task.register("tree_search", build=build_tree_search_parser)
    def run_tree_search(self, *args, rerun_incomplete: bool, overwrite_check: bool, **kwargs):
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

    def _parse_log(self):
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
            df.to_csv(f"{self.analysis_output}/{command_name}.csv", index=False)

    def _analyze(self):
        self.dfs: list[pd.DataFrame] = []

        # compute the min and avg
        for i, command_name in enumerate(self.commands.keys()):
            df = pd.read_csv(f"{self.analysis_output}/{command_name}.csv")
            df[f"AvgScore_{i}"] = df["Scores"].apply(lambda x: round(sum(eval(x)) / len(eval(x))))
            df[f"AvgRuntime_{i}"] = df["Runtimes"].apply(lambda x: sum(eval(x)) / len(eval(x)))
            df[f"MinScore_{i}"] = df["Scores"].apply(lambda x: min(eval(x)))
            df.drop(columns=["Scores", "Runtimes"], inplace=True)
            self.dfs.append(df)

        combined_df = self.dfs[0].copy()
        for i in range(1, len(self.dfs)):
            combined_df = pd.merge(combined_df, self.dfs[i], on="MSA")
        self.combined_df = combined_df
        self.combined_cmds = list(self.commands.keys())

        # add TNT
        if self.tnt_csv:
            self.combined_cmds.append("TNT")
            tnt_df = pd.read_csv(self.tnt_csv)
            tnt_df["Time (secs)"] = tnt_df["Time (secs)"] / 3600  # convert to hours
            # rename columns
            tnt_df[f"MinScore_{len(self.dfs)}"] = tnt_df["MP Score"]
            tnt_df.rename(columns={"Data": "MSA", "MP Score": f"AvgScore_{len(self.dfs)}", f"Time (secs)": f"AvgRuntime_{len(self.dfs)}"}, inplace=True)
            tnt_df["MSA"] = tnt_df["MSA"].apply(lambda x: x + ".phy")

            # merge
            combined_df = pd.merge(combined_df, tnt_df, on="MSA")

        logger.info(f"Number of MSAs: {len(combined_df)}/{len(self.dataset)}")


        # calculate the best score
        combined_df["BestScore"] = combined_df[[f"MinScore_{i}" for i in range(len(self.combined_cmds))]].min(axis=1)

        # calculate everything else
        self.best_score_hits = []
        self.total_runtimes = []
        for i, command_name in enumerate(self.combined_cmds):
            best_score_hit = (combined_df[f"AvgScore_{i}"] == combined_df["BestScore"]).sum()
            total_runtime = combined_df[f"AvgRuntime_{i}"].sum()
            self.best_score_hits.append(best_score_hit)
            self.total_runtimes.append(total_runtime)

    def _plot_analysis(self):
        # Plotting
        x = np.arange(len(self.combined_cmds))
        width = 0.35
        fig, ax1 = plt.subplots(figsize=(10, 6))
        color1 = 'tab:blue'
        color2 = 'tab:red'
        ax1.set_xlabel('Command')
        ax1.set_ylabel('Best Score Hit', color=color1)
        bar1 = ax1.bar(x - width/2, self.best_score_hits, width, color=color1, alpha=0.6, label='Best Score Hit')
        ax1.tick_params(axis='y', labelcolor=color1)
        ax1.set_xticks(x)
        ax1.set_xticklabels(self.combined_cmds)
        ax1.bar_label(bar1)

        ax2 = ax1.twinx()
        ax2.set_ylabel('Total Runtime (h)', color=color2)
        bar2 = ax2.bar(x + width/2, self.total_runtimes, width, color=color2, alpha=0.4, label='Total Runtime')
        ax2.tick_params(axis='y', labelcolor=color2)
        ax2.bar_label(bar2, fmt='%.2f')

        fig.tight_layout()
        fig.suptitle(f'{len(self.combined_df)}/{len(self.dataset)} MSA, {len(self.seeds)} seeds')
        fig.subplots_adjust(left=0.1, right=0.9, top=0.9, bottom=0.1)
        fig.savefig(f"{self.analysis_output}/analysis_results.svg")
        # plt.show()

    @Task.register("analyze", build=build_analysis_parser)
    def run_analysis(self, analysis_output: str, no_parse: bool, tnt_csv: Optional[str] = None, **kwargs):
        self.analysis_output = analysis_output
        self.tnt_csv = tnt_csv
        if not no_parse:
            self._parse_log()
        self._analyze()
        self._plot_analysis()
