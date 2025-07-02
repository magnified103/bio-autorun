import multiprocessing as mp
from contextlib import contextmanager

from bio_autorun.job import Job, JobStatus


class BaseExecutorConfig:
    pass


class BaseExecutor:
    def __init__(self, config: BaseExecutorConfig):
        self.config = config
        self._event_queue = mp.JoinableQueue()
        self._event_subscriptions = []
        self._event_loop_worker = mp.Process(target=self._event_loop, daemon=True)

    def enter_loop(self):
        self._event_loop_worker.start()

    def exit_loop(self, exc_type=None, exc_value=None, traceback=None):
        self._event_queue.close()
        self._event_queue.join()
        self._event_loop_worker.terminate()
        self._event_loop_worker.join()
        if exc_type is not None:
            raise exc_type(exc_value).with_traceback(traceback)

    @contextmanager
    def acquire(self):
        """
        Return a context manager that will start the executor and stop it when done.
        :return:
        """
        self.enter_loop()
        try:
            yield
        except Exception as e:
            return self.exit_loop(exc_type=type(e), exc_value=e, traceback=e.__traceback__)
        else:
            return self.exit_loop()

    def event_subscribe(self, status: JobStatus, callback):
        """
        Subscribe to a job event. All callbacks will be executed in a separate process.
        :param status:
        :param callback:
        :return:
        """
        self._event_subscriptions.append((status, callback))

    def event_publish(self, status: JobStatus, job: Job):
        self._event_queue.put((status, job))

    def _event_loop(self):
        while True:
            try:
                status, job = self._event_queue.get()
            except:
                break
            for s, callback in self._event_subscriptions:
                if s == status:
                    callback(job)
            self._event_queue.task_done()

    def submit(self, job: Job):
        raise NotImplementedError


class ExecutorFactory:
    registry: dict[type, type] = {}

    @classmethod
    def register(cls, config_class: type, executor_class: type):
        cls.registry[config_class] = executor_class

    @classmethod
    def create_executor(cls, config: BaseExecutorConfig) -> 'BaseExecutor':
        if type(config) not in cls.registry:
            raise ValueError(f"No executor registered for config type {type(config)}")
        return cls.registry[type(config)](config)
