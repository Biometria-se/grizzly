"""
@anchor pydoc:grizzly.tasks.keystore Keystore task
This tasks sets and gets values from a distributed keystore.

## Step implementations

@TODO

## Statistics

This task does not have any request statistics entries.

## Arguments

*
"""
from __future__ import annotations
from typing import Any, Literal, TYPE_CHECKING

from . import GrizzlyTask, grizzlytask

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.scenarios import GrizzlyScenario

Action = Literal['get', 'set']


class KeystoreTask(GrizzlyTask):
    key: str
    variable: str
    action: Action

    def __init__(self, key: str, variable: str, action: Action) -> None:
        super().__init__()

        self.key = key
        self.variable = variable
        self.action = action

    def __call__(self) -> grizzlytask:
        @grizzlytask
        def task(parent: GrizzlyScenario) -> Any:
            return None

        return task
