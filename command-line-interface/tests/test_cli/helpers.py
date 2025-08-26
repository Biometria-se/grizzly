"""Test command-line-interface helpers."""

from __future__ import annotations

import os
import subprocess
import sys
from abc import ABCMeta
from contextlib import contextmanager, suppress
from importlib.metadata import version
from pathlib import Path
from typing import TYPE_CHECKING, Any

from behave.model import Scenario, Step
from grizzly_cli.utils import rm_rf

if TYPE_CHECKING:
    from collections.abc import Generator

__all__ = ['rm_rf']


def CaseInsensitive(value: str) -> object:  # noqa: N802
    class Wrapped(str):
        __slots__ = ()

        def __eq__(self, other: object) -> bool:
            return isinstance(other, str) and other.lower() == value.lower()

        def __ne__(self, other: object) -> bool:
            return not self.__eq__(other)

        def __neq__(self, other: object) -> bool:
            return self.__ne__(other)

        def __hash__(self) -> int:
            return hash(self)

    return Wrapped()


def run_command(command: list[str], env: dict[str, str] | None = None, cwd: Path | None = None, stdin: str | None = None) -> tuple[int, list[str]]:
    output: list[str] = []
    if env is None:
        env = os.environ.copy()

    if cwd is None:
        cwd = Path.cwd()

    process = subprocess.Popen(
        command,
        env=env,
        cwd=cwd,
        stderr=subprocess.STDOUT,
        stdout=subprocess.PIPE,
        stdin=subprocess.PIPE,
    )

    if stdin is not None:
        assert process.stdin is not None
        process.stdin.write(f'{stdin}\n'.encode())
        process.stdin.close()

    try:
        while process.poll() is None:
            stdout = process.stdout
            if stdout is None:
                break

            buffer = stdout.readline()
            if not buffer:
                break

            line = buffer.decode('utf-8')
            if sys.platform == 'win32':
                line = line.replace(os.linesep, '\n')

            output.append(line)

        process.terminate()
    except KeyboardInterrupt:
        pass
    finally:
        with suppress(Exception):
            process.kill()

    process.wait()

    return process.returncode, output


def create_scenario(name: str, background_steps: list[str], steps: list[str]) -> Scenario:
    scenario = Scenario('', '', '', name)

    for background_step in background_steps:
        [keyword, name] = background_step.split(' ', 1)
        behave_step = Step('', '', keyword.strip(), keyword.strip(), name.strip())
        if scenario._background_steps is None:
            scenario._background_steps = []
        scenario._background_steps.append(behave_step)

    for step in steps:
        [keyword, name] = step.split(' ', 1)
        behave_step = Step('', '', keyword.strip(), keyword.strip(), name.strip())
        scenario.steps.append(behave_step)

    return scenario


def get_current_version() -> tuple[str, str]:
    return version('grizzly-loadtester-cli'), version('grizzly-loadtester-common')


@contextmanager
def cwd(path: Path) -> Generator[None, None, None]:
    current_cwd = Path.cwd()
    os.chdir(path)

    try:
        yield
    finally:
        os.chdir(current_cwd)


def ANY(*cls: type, message: str | None = None) -> object:  # noqa: N802
    """Compare equal to everything, as long as it is of the same type."""

    class WrappedAny(metaclass=ABCMeta):  # noqa: B024
        def __eq__(self, other: object) -> bool:
            if len(cls) < 1:
                return True

            return isinstance(other, cls) and (message is None or (message is not None and message in str(other)))

        def __ne__(self, other: object) -> bool:
            return not self.__eq__(other)

        def __neq__(self, other: object) -> bool:
            return self.__ne__(other)

        def __repr__(self) -> str:
            c = cls[0] if len(cls) == 1 else cls
            representation: list[str] = [f'<ANY({c})', '>']

            if message is not None:
                representation.insert(-1, f", message='{message}'")

            return ''.join(representation)

        def __hash__(self) -> int:
            return hash(self)

    for c in cls:
        WrappedAny.register(c)

    return WrappedAny()


def SOME(cls: type, *value: Any, **values: Any) -> object:  # noqa: N802
    class WrappedSome:
        def __eq__(self, other: object) -> bool:
            if issubclass(cls, dict):

                def get_value(other: Any, attr: str) -> Any:
                    return other.get(attr)
            else:

                def get_value(other: Any, attr: str) -> Any:
                    return getattr(other, attr)

            return isinstance(other, cls) and all(get_value(other, attr) == value for attr, value in values.items())

        def __ne__(self, other: object) -> bool:
            return not self.__eq__(other)

        def __neq__(self, other: object) -> bool:
            return self.__ne__(other)

        def __repr__(self) -> str:
            info = ', '.join([f'{key}={value}' for key, value in values.items()])
            return f'<SOME({cls}, {info})>'

        def __hash__(self) -> int:
            return hash(self)

    if len(value) > 0 and len(values) > 0:
        message = 'cannot use both positional and named arguments'
        raise RuntimeError(message)

    if len(values) < 1 and len(value) < 1:
        raise AttributeError(name='values', obj=str(type))

    if len(value) > 1:
        message = 'can only use 1 positional argument'
        raise RuntimeError(message)

    if len(value) > 0 and isinstance(value[0], dict):
        values = {**value[0]}

    return WrappedSome()
