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


class TreeSearchAnalysis(Task):
    def __init__(self, *args, command_names: list[str], dataset: Union[Dataset, Iterable[MSA]], seeds: list[int],
                 csv_dirs: list[str], analysis_output: str, **kwargs):
        super().__init__(*args, **kwargs)
        self.command_names = command_names
        self.dataset = dataset
        self.seeds = seeds
        self.csv_dirs = csv_dirs
        self.analysis_output = analysis_output

    def _analyze(self):
        self.dfs: list[pd.DataFrame] = []

        # compute the min and avg
        for i, command_name in enumerate(self.command_names):
            df = None
            for d in self.csv_dirs:
                try:
                    df = pd.read_csv(f"{d}/{command_name}.csv")
                    break
                except:
                    pass
            df[f"AvgScore_{i}"] = df["Scores"].apply(lambda x: round(sum(eval(x)) / len(eval(x))))
            df[f"AvgRuntime_{i}"] = df["Runtimes"].apply(lambda x: sum(eval(x)) / len(eval(x)))
            df[f"MinScore_{i}"] = df["Scores"].apply(lambda x: min(eval(x)))
            df.drop(columns=["Scores", "Runtimes"], inplace=True)
            self.dfs.append(df)

        combined_df = self.dfs[0].copy()
        for i in range(1, len(self.dfs)):
            combined_df = pd.merge(combined_df, self.dfs[i], on="MSA")
        self.combined_df = combined_df
        self.combined_cmds = list(self.command_names)

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

    def __call__(self, *args, **kwargs):
        self._analyze()
        self._plot_analysis()
