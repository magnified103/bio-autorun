import argparse


def main():
    parser = argparse.ArgumentParser(description="Generate a Slurm worker script.")
    parser.add_argument("connect_uri", type=str, help="The URI to connect to the scheduler server.")
    parser.add_argument("api_key", type=str, help="The API key for authentication.")
    parser.add_argument("--job_name", type=str, default=None, help="The name of the job.")
    parser.add_argument("-o", "--output", type=argparse.FileType("x"), default="batch.sh")
    args = parser.parse_args()

    args.output.write("#!/bin/bash\n")
    if args.job_name is not None:
        args.output.write(f"#SBATCH --job-name={args.job_name}\n")
    args.output.write("#SBATCH --output=slurm-log/slurm-%A.out\n")
    args.output.write(f"srun /usr/bin/env python3 -m bio_autorun.scheduler.worker '{args.connect_uri}' '{args.api_key}'\n")
