"""Base grizzly-cli module."""

from __future__ import annotations

from os import environ
from pathlib import Path
from typing import TYPE_CHECKING, ClassVar

try:
    from grizzly_cli.__version__ import __version__
except ImportError:
    __version__ = 'unknown'

try:
    from grizzly_common.__version__ import __version__ as __common_version__
except ImportError:
    __common_version__ = 'unknown'

if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Callable

    from behave.model import Scenario

    from grizzly_cli.argparse import ArgumentSubParser

EXECUTION_CONTEXT = Path.cwd().as_posix()

STATIC_CONTEXT = Path.joinpath(Path(__file__).parent.absolute(), 'static').as_posix()

MOUNT_CONTEXT = environ.get('GRIZZLY_MOUNT_CONTEXT', EXECUTION_CONTEXT)

PROJECT_NAME = Path(EXECUTION_CONTEXT).name

SCENARIOS: list[Scenario] = []

FEATURE_DESCRIPTION: str | None = None


class register_parser:
    registered: ClassVar[list[Callable[[ArgumentSubParser], None]]] = []
    order: int | None

    def __init__(self, order: int | None = None) -> None:
        self.order = order

    def __call__(self, func: Callable[[ArgumentSubParser], None]) -> Callable[[ArgumentSubParser], None]:
        if self.order is not None:
            self.registered.insert(self.order - 1, func)
        else:
            self.registered.append(func)

        return func


__all__ = ['__common_version__', '__version__']
