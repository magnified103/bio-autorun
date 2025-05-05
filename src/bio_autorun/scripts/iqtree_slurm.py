import argparse
import importlib.util
import logging
import os


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
    parser.add_argument("--slurm-output", default="job.sh", help="Path to the slurm output file", type=argparse.FileType("w"))
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
    # - NAME: the name of the experiment
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

    if os.path.exists(settings.OUTPUT_DIR):
        logger.warning("Output directory already exists.")
    else:
        os.makedirs(settings.OUTPUT_DIR)

    number_of_jobs = 0

    for command_name, command_str in settings.COMMANDS.items():
        for seed in settings.SEEDS:
            for data in data_names:
                if args.only_skipped and data not in settings.SKIPPED_DATA:
                    continue
                if not args.only_skipped and data in settings.SKIPPED_DATA:
                    continue
                if settings.INCLUDED_DATA is not None and data not in settings.INCLUDED_DATA:
                    continue
                job_name = f"{data}_{command_name}_{seed}"
                prefix = os.path.join(settings.OUTPUT_DIR, job_name)
                if os.path.exists(prefix + ".iqtree"):
                    logger.info(f"Job {job_name} already exists. Skipping.")
                    continue
                job_cmd = f"{command_str} -s {os.path.join(settings.DATA_DIR, data)} -m {settings.MODELS[data]} --prefix {prefix} --seed {seed}"
                if data in settings.ITERS:
                    job_cmd += f" -n {settings.ITERS[data]}"
                args.output.write(job_cmd + "\n")
                number_of_jobs += 1

    args.slurm_output.write("#!/bin/bash\n")
    args.slurm_output.write(f"#SBATCH --job-name={settings.NAME}\n")
    args.slurm_output.write(f"#SBATCH --ntasks=1\n")
    args.slurm_output.write(f"#SBATCH --cpus-per-task=1\n")
    args.slurm_output.write(f"#SBATCH --array=1-{number_of_jobs}\n")
    args.slurm_output.write(f"#SBATCH --output=slurm-log/slurm-%A_%a.out\n")
    args.slurm_output.write(f"command=$(sed \"${{SLURM_ARRAY_TASK_ID}}q;d\" {args.output.name})\n")
    args.slurm_output.write("eval $command\n")
