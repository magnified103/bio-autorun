import argparse
from typing import Callable

from bio_autorun.executors import BaseExecutorConfig, ExecutorFactory


class Task:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()

    def __init__(self, executor_config: BaseExecutorConfig, **kwargs):
        self.executor = ExecutorFactory.create_executor(executor_config)

    @classmethod
    def register(cls, name, build: Callable[[argparse.ArgumentParser], None] = lambda _: None, **kwargs):
        def decorator(func):
            parser = cls.subparsers.add_parser(name, **kwargs)
            parser.set_defaults(func=func)
            build(parser)
            def wrapper(*args, **kwargs):
                raise RuntimeError("This method should not be called directly.")
            return wrapper
        return decorator

    def main(self):
        args = self.parser.parse_args()
        if not hasattr(args, 'func'):
            self.parser.print_help()
            return
        func = args.func
        del args.func
        return func(self, **vars(args))
