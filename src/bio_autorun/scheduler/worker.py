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

        if job.stdin:
            stdin = open(job.stdin, 'r')
        else:
            if job.stdin_str:
                stdin = subprocess.PIPE
            else:
                stdin = subprocess.DEVNULL
        
        if job.stdout:
            stdout = open(job.stdout, 'w')
        else:
            stdout = subprocess.DEVNULL
        
        if job.stderr:
            if job.stderr == "stdout":
                stderr = subprocess.STDOUT
            else:
                stderr = open(job.stderr, 'w')
        else:
            stderr = subprocess.DEVNULL

        # Start the job
        proc = subprocess.Popen(
            job.cmd,
            cwd=job.cwd,
            env=job.env,
            shell=job.shell,
            stdin=stdin,
            stdout=stdout,
            stderr=stderr,
        )

        if job.stdin_str:
            inp = job.stdin_str.encode('utf-8')
            proc.communicate(input=inp)

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
