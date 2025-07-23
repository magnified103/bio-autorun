from abc import abstractmethod
import argparse
from typing import Callable

from bio_autorun.executors import BaseExecutorConfig, ExecutorFactory


class Task:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()

    def __init__(self, name, executor_config: BaseExecutorConfig, **kwargs):
        self.executor = ExecutorFactory.create_executor(executor_config)
        self.parser = Task.subparsers.add_parser(name)
        self.parser.set_defaults(func=self.__call__)

    @abstractmethod
    def __call__(self, *args, **kwargs):
        raise NotImplementedError()

    @classmethod
    def main(cls):
        args = cls.parser.parse_args()
        if not hasattr(args, 'func'):
            cls.parser.print_help()
            return
        func = args.func
        del args.func
        return func(**vars(args))
