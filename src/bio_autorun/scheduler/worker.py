from datetime import datetime, timezone
import logging
import subprocess
import sys

import requests

from bio_autorun.job import Job, JobStatus


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
MAX_RETRIES = 3
retries = 0


while retries < MAX_RETRIES:
    try:
        response = requests.post(f"{sys.argv[1]}/get_job", headers={"X-API-KEY": sys.argv[2]}, timeout=10)
        response.raise_for_status()
        job = Job.from_json(response.json())

        logger.info(f"Starting job: {job.name} with command: {job.cmd}")
        job.start_time = datetime.now(timezone.utc)
        job.status = JobStatus.STARTED

        # Start the job
        proc = subprocess.Popen(
            job.cmd,
            cwd=job.cwd,
            env=job.env,
            shell=job.shell,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

        # Wait for the job to finish
        proc.wait()

        job.end_time = datetime.now(timezone.utc)
        job.status = JobStatus.COMPLETED
        job.exit_code = proc.returncode
        logger.info(f"Job completed: {job.name} with exit code {job.exit_code}")

        # Reset the retries counter
        retries = 0
    except Exception as e:
        logger.error(f"Error: {e}")
        retries += 1
        continue
