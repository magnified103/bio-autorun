from datetime import datetime, timezone
from functools import wraps
import json
import logging
from queue import SimpleQueue
import threading

from flask import Flask, request

from bio_autorun.job import Job, JobStatus

logger = logging.getLogger(__name__)
app = Flask(__name__)
q: SimpleQueue[Job] = SimpleQueue()
API_KEY = "abc123"  # Change this to your actual secret key
stop = threading.Event()


def require_api_key(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        key = request.headers.get('X-API-KEY')
        if key != API_KEY:
            return {"error": "Unauthorized"}, 401
        return f(*args, **kwargs)
    return decorated


@app.route('/add_job', methods=['POST'])
@require_api_key
def add_job():
    body = request.get_json()
    try:
        job = Job.from_json(body)
        job.status = JobStatus.QUEUED
        job.queued_time = datetime.now(timezone.utc)
    except Exception as e:
        logger.error(f"Error adding job: {body}")
        return {"error": str(e)}, 400
    q.put(job)
    logger.info(f"Job added: {job.name}")
    return {"message": "Job added successfully"}, 200


@app.route('/get_job', methods=['POST'])
@require_api_key
def get_job():
    try:
        if stop.is_set():
            job = q.get_nowait()
        else:
            job = q.get()
    except Exception as e:
        logger.error(f"Error getting job: {e}")
        return {"error": str(e)}, 500
    return json.dumps(job.to_json()), 200


@app.route('/stop_server', methods=['POST'])
@require_api_key
def stop_server():
    stop.set()
    return {"message": "Server stopping"}, 200
