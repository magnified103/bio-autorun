from typing_extensions import override

from bio_autorun.executors.base import BaseExecutor, BaseExecutorConfig, ExecutorFactory
from bio_autorun.job import Job


class CatExecutorConfig(BaseExecutorConfig):
    def __init__(self, output_file: str, **kwargs):
        super().__init__(**kwargs)
        self.output_file = output_file


class CatExecutor(BaseExecutor):
    config: CatExecutorConfig

    def __init__(self, config: CatExecutorConfig):
        super().__init__(config)
        self.output = open(config.output_file, 'x')

    @override
    def submit(self, job: Job):
        self.output.write(f"{job.to_json()}\n")

    @override
    def exit_loop(self, exc_type=None, exc_value=None, traceback=None):
        self.output.close()
        super().exit_loop(exc_type, exc_value, traceback)


ExecutorFactory.register(CatExecutorConfig, CatExecutor)
