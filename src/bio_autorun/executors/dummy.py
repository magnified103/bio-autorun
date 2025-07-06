from typing_extensions import override

from bio_autorun.executors.base import BaseExecutor, BaseExecutorConfig, ExecutorFactory
from bio_autorun.job import Job


class DummyExecutorConfig(BaseExecutorConfig):
    pass


class DummyExecutor(BaseExecutor):
    config: DummyExecutorConfig

    @override
    def submit(self, job: Job):
        pass


ExecutorFactory.register(DummyExecutorConfig, DummyExecutor)
