"""Utility decorators used by :mod:`molq`.

This module defines :class:`YieldDecorator`, a base class that simplifies
building decorators that operate on generator-based tasks. It also provides
a concrete :class:`CmdlineDecorator` for running shell commands in the middle
of a generator.
"""

import subprocess
from abc import ABC, abstractmethod
from functools import wraps
from inspect import isgeneratorfunction, signature
from pathlib import Path
from typing import Any, Callable, Generator


class YieldDecorator(ABC):
    """Base class for decorators that interact with generator-based tasks."""

    def __call__(self, func: Callable):
        """Wrap ``func`` so that ``yield`` points can be intercepted."""

        @wraps(func)
        def wrapper(*args, **kwargs):
            if not isgeneratorfunction(func):
                self.before_call(func)
                result = func(*args, **kwargs)
                return self.after_call(result)

            self.before_call(func)
            generator: Generator = func(*args, **kwargs)
            result = None
            try:
                yield_result: Any = next(generator)
                while True:
                    yield_result = self.validate_yield(yield_result)
                    result = self.after_yield(yield_result)
                    yield_result = generator.send(result)
            except StopIteration as e:
                result = e.value
            return self.after_call(result)

        # get the return type and set it as the return type of the wrapper
        wrapper.__annotations__["return"] = signature(func).return_annotation
        return wrapper

    def before_call(self, *args, **kwargs):
        """Hook executed before the wrapped function is called."""
        ...

    def validate_yield(self, yield_result: Any) -> Any:
        """Validate ``yield`` content sent by the wrapped generator."""
        return yield_result

    @abstractmethod
    def after_yield(self, yield_result: Any) -> Any:
        """Process ``yield`` data and return a value to send back."""
        pass

    def after_call(self, result: Any) -> Any:
        """Hook executed after the wrapped function finishes."""
        return result


class CmdlineDecorator(YieldDecorator):
    """Execute a shell command inside a generator based task."""

    def validate_yield(self, config: dict) -> dict:
        """Check the configuration dictionary yielded by the task."""
        if not isinstance(config, dict):
            raise TypeError("cmdline config must be a dict")
        if "cmd" not in config:
            raise ValueError("`cmd` is required for cmdline execution")
        return config

    def after_yield(self, config: dict) -> subprocess.CompletedProcess:
        """Run the command and return the resulting process object."""
        cmd = config["cmd"]
        cwd = Path(config.get("cwd", Path.cwd()))
        block = config.get("block", True)

        if isinstance(cmd, str):
            cmd = [cmd]

        if block:
            return subprocess.run(cmd, cwd=cwd, capture_output=True)
        else:
            proc = subprocess.Popen(cmd, cwd=cwd)
            return proc


# Convenient instance used as ``@cmdline`` decorator
cmdline = CmdlineDecorator()
