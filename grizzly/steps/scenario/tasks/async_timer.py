"""@anchor pydoc:grizzly.steps.scenario.tasks.async_timer Asynchronous Timer
This module contains step implementations for the {@pylink grizzly.tasks.async_timer} task.
"""
from __future__ import annotations

from typing import cast

from grizzly.context import GrizzlyContext
from grizzly.tasks import AsyncTimerTask
from grizzly.types.behave import Context, then


@then('start document timer with name "{name}" for id "{tid}" and version "{version}"')
def step_task_async_timer_start(context: Context, name: str, tid: str, version: str) -> None:
    """Start an asynchrounous timer."""
    grizzly = cast(GrizzlyContext, context.grizzly)

    grizzly.scenario.tasks.add(AsyncTimerTask(name, tid, version, 'start'))


@then('stop document timer with name "{name}" for id "{tid}" and version "{version}"')
def step_task_async_timer_stop_name(context: Context, name: str, tid: str, version: str) -> None:
    """Start an asynchrounous timer."""
    grizzly = cast(GrizzlyContext, context.grizzly)

    grizzly.scenario.tasks.add(AsyncTimerTask(name, tid, version, 'stop'))


@then('stop document timer for id "{tid}" and version "{version}"')
def step_task_async_timer_stop_tid(context: Context, tid: str, version: str) -> None:
    """Start an asynchrounous timer."""
    grizzly = cast(GrizzlyContext, context.grizzly)

    grizzly.scenario.tasks.add(AsyncTimerTask(None, tid, version, 'stop'))
