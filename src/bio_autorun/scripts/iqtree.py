import argparse
import importlib.util
import logging
import os

from bio_autorun.executors.local import LocalExecutor, LocalExecutorConfig
from bio_autorun.job import Job, JobStatus


def import_settings(settings_path):
    spec = importlib.util.spec_from_file_location("settings", settings_path)
    settings = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(settings)
    return settings

logger = logging.getLogger("iqtree")


def get_data_file(data_dir):
    data = []
    for name in os.listdir(data_dir):
        if name.endswith(".phy"):
            data.append(name)
    return data

def main():
    parser = argparse.ArgumentParser(description="Load a settings.py file.")
    parser.add_argument(
        "-s",
        "--settings",
        required=True,
        help="Path to the settings.py file to import"
    )
    parser.add_argument("--log-file", default="iqtree.log", help="Path to the log file")
    args = parser.parse_args()
    logging.basicConfig(filename=args.log_file, level=logging.INFO)
    settings = import_settings(args.settings)

    # the settings.py should define the following variables:
    # - DATA_DIR: the directory containing the data files
    # - OUTPUT_DIR: the directory to save the output files
    # - SEEDS: a list of integers indicating the initial seeds
    # - COMMANDS: a dictionary where keys and values are the command names and their corresponding command strings
    # - MODELS: a dictionary mapping the data files to the optimal models

    data_names = get_data_file(settings.DATA_DIR)
    logger.info(f"Data files found: {len(data_names)}")
    for data in data_names:
        if data not in settings.MODELS:
            raise RuntimeError(f"Data file {data} not found in models mapping.")

    if os.path.exists(settings.OUTPUT_DIR):
        raise RuntimeError("Output directory already exists.")
    os.makedirs(settings.OUTPUT_DIR)

    config = LocalExecutorConfig(max_workers=4)
    executor = LocalExecutor(config)
    # executor.event_subscribe(JobStatus.SUBMITTED, lambda job: logging.info(f"Job {job} submitted"))
    executor.event_subscribe(JobStatus.QUEUED, lambda job: logging.info(f"Job {job} queued"))
    executor.event_subscribe(JobStatus.STARTED, lambda job: logging.info(f"Job {job} started at {job.start_time}"))
    executor.event_subscribe(JobStatus.COMPLETED, lambda job: logging.info(f"Job {job} ended at {job.end_time}, exit code: {job.exit_code}"))

    with executor.acquire():
        for command_name, command_str in settings.COMMANDS.items():
            for seed in settings.SEEDS:
                for data in data_names:
                    job_name = f"{data}_{command_name}_{seed}"
                    job_cmd = f"{command_str} -s {os.path.join(settings.DATA_DIR, data)} -m {settings.MODELS[data]} --prefix {os.path.join(settings.OUTPUT_DIR, job_name)} --seed {seed}"
                    job = Job(name=job_name, cmd=job_cmd, shell=True)
                    executor.submit(job)
