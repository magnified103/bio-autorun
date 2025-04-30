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
        default="settings.py",
        help="Path to the settings.py file to import"
    )
    parser.add_argument("--only-skipped", action="store_true", help="Only run skipped data")
    parser.add_argument("-o", "--output", default="iqtree.cmd", help="Path to the output script", type=argparse.FileType("w"))
    parser.add_argument("--log-file", default="iqtree.log", help="Path to the log file")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, handlers=[
        logging.FileHandler(args.log_file),
        logging.StreamHandler()
    ])
    settings = import_settings(args.settings)

    # the settings.py should define the following variables:
    # - WORKERS: the number of workers
    # - DATA_DIR: the directory containing the data files
    # - OUTPUT_DIR: the directory to save the output files
    # - SEEDS: a list of integers indicating the initial seeds
    # - COMMANDS: a dictionary where keys and values are the command names and their corresponding command strings
    # - MODELS: a dictionary mapping the data files to the optimal models
    # - ITERS: a dictionary mapping the data files to the number of iterations (optional)
    # - SKIPPED_DATA: a list of data files to skip (optional)

    data_names = get_data_file(settings.DATA_DIR)
    logger.info(f"Data files found: {len(data_names)}")
    for data in data_names:
        if data not in settings.MODELS:
            raise RuntimeError(f"Data file {data} not found in models mapping.")

    # if os.path.exists(settings.OUTPUT_DIR):
    #     raise RuntimeError("Output directory already exists.")
    # os.makedirs(settings.OUTPUT_DIR)

    for command_name, command_str in settings.COMMANDS.items():
        for seed in settings.SEEDS:
            for data in data_names:
                if args.only_skipped and data not in settings.SKIPPED_DATA:
                    continue
                if not args.only_skipped and data in settings.SKIPPED_DATA:
                    continue
                job_name = f"{data}_{command_name}_{seed}"
                job_cmd = f"{command_str} -s {os.path.join(settings.DATA_DIR, data)} -m {settings.MODELS[data]} --prefix {os.path.join(settings.OUTPUT_DIR, job_name)} --seed {seed}"
                if data in settings.ITERS:
                    job_cmd += f" -n {settings.ITERS[data]}"
                args.output.write(job_cmd + "\n")
