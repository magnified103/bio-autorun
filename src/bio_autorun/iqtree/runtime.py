import re
import logging
from bio_autorun.msa import MSA, treebase_load
from bio_autorun.iqtree.settings import Settings
from dataclasses import dataclass
import os

logger = logging.getLogger(__name__)

type JobDesc = tuple[str, MSA, int]


@dataclass(frozen=True)
class JobResult:
    best_score: float
    cpu_time: float
    iters: int
    log_path: str


class JobResultParser:
    def parse(self, log_path: str):
        with open(log_path, "r") as f:
            content = f.read()

            # Extract best score
            score_match = re.search(r"^BEST SCORE FOUND : (-?\d+\.\d+)$", content, re.M)
            self.best_score = float(score_match.group(1)) if score_match else None
            if self.best_score is None:
                # program got interrupted
                logger.warning(f"Best score not found in {log_path}")
                score_match = re.findall(r"^BETTER TREE FOUND at iteration \d+: (-?\d+\.\d+)$", content, re.M)
                self.best_score = float(score_match[-1]) if score_match else None

            # Extract CPU time
            cpu_time_match = re.search(r"^CPU time used for tree search: (\d+\.\d+) sec", content, re.M)
            self.cpu_time = float(cpu_time_match.group(1)) if cpu_time_match else None
            if self.cpu_time is None:
                # program got interrupted, assuming that the tree search was completed
                logger.warning(f"CPU time not found in {log_path}")
                cpu_time_match = re.search(r"^TREE SEARCH COMPLETED AFTER \d+ ITERATIONS \/ Time: (?:(\d+)h:)?(?:(\d+)m:)?(?:(\d+)s)$", content, re.M)
                self.cpu_time = 0
                if cpu_time_match.group(1):
                    self.cpu_time += int(cpu_time_match.group(1)) * 3600
                if cpu_time_match.group(2):
                    self.cpu_time += int(cpu_time_match.group(2)) * 60
                if cpu_time_match.group(3):
                    self.cpu_time += int(cpu_time_match.group(3))

            # extract number of iters
            iters_match = re.search(r"^TREE SEARCH COMPLETED AFTER (\d+) ITERATIONS", content, re.M)
            self.iters = int(iters_match.group(1)) if iters_match else None
        return JobResult(
            best_score=self.best_score,
            cpu_time=self.cpu_time,
            iters=self.iters,
            log_path=log_path,
        )


class BenchAnalysis:
    def __init__(self):
        pass



class BenchAnalyzer:
    def parse_dataset(self):
        self.msa_list = treebase_load(self.settings)

    def parse_log(self):
        self.job_results: dict[JobDesc, JobResult] = {}
        job_result_parser = JobResultParser()
        for command_name in self.settings.commands.keys():
            for seed in self.settings.seeds:
                for msa in self.msa_list:
                    log_path = f"{self.settings.output_dir}/{command_name}_{msa.name}_{seed}.log"
                    if not os.path.exists(log_path):
                        continue
                    job_result = job_result_parser.parse(log_path)
                    job_desc = JobDesc(msa=msa, algo=command_name, seed=seed)
                    self.job_results[job_desc] = job_result

    def parse(self, settings: Settings):
        self.settings = settings
        self.parse_dataset()
        self.parse_log()

    def analyze(self) -> BenchAnalysis:
        pass
