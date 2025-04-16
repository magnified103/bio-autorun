import multiprocessing as mp
from contextlib import contextmanager

from bio_autorun.job import Job, JobStatus


class ExecutorConfig:
    pass


class Executor:
    def __init__(self, config: ExecutorConfig):
        self.config = config
        self._event_queue = mp.JoinableQueue()
        self._event_subscriptions = []
        self._event_loop_worker = mp.Process(target=self._event_loop, daemon=True)

    def enter_loop(self):
        self._event_loop_worker.start()

    def exit_loop(self, exc_type=None, exc_value=None, traceback=None):
        self._event_queue.join()
        self._event_queue.close()
        self._event_loop_worker.terminate()
        self._event_loop_worker.join()
        if exc_type:
            raise exc_type(exc_value).with_traceback(traceback)

    @contextmanager
    def acquire(self):
        self.enter_loop()
        try:
            yield
        except Exception as e:
            return self.exit_loop(exc_type=type(e), exc_value=e, traceback=e.__traceback__)
        else:
            return self.exit_loop()

    def event_subscribe(self, status: JobStatus, callback):
        self._event_subscriptions.append((status, callback))

    def event_publish(self, status: JobStatus, job: Job):
        self._event_queue.put((status, job))

    def _event_loop(self):
        while True:
            status, job = self._event_queue.get()
            for s, callback in self._event_subscriptions:
                if s == status:
                    callback(job)
            self._event_queue.task_done()

    def submit(self, job: Job):
        raise NotImplementedError
