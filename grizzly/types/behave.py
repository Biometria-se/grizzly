"""Behave types frequently used in grizzly."""
from __future__ import annotations

from functools import wraps
from typing import Any, Callable, Dict, List, TypeVar, cast

import behave
from behave.model import Feature, Row, Scenario, Status, Step, Table
from behave.runner import Context

from grizzly.exceptions import StepError

StepFunctionType = TypeVar('StepFunctionType', bound=Callable[..., None])
StepFunctionWrapperType = Callable[[StepFunctionType], StepFunctionType]

def step_wrapper(step_func: Callable[[str], StepFunctionWrapperType], pattern: str) -> StepFunctionWrapperType:
    @wraps(step_func)
    def wrapper(func: StepFunctionType) -> StepFunctionType:
        return cast(StepFunctionType, step_func(pattern)(error_handler(func)))

    return wrapper

def error_handler(func: StepFunctionType) -> StepFunctionType:
    @wraps(func)
    def wrapper(context: Context, *args: Any, **kwargs: Any) -> None:
        try:
            return func(context, *args, **kwargs)
        except Exception as e:
            if not hasattr(context, 'exceptions'):
                context.exceptions = {}

            exception = StepError(e, context.step).with_traceback(e.__traceback__) if isinstance(e, AssertionError) else e

            cast(Dict[str, List[Exception]], context.exceptions).update({
                context.scenario.name: [*context.exceptions.get(context.scenario.name, []), exception],
            })

            if not isinstance(e, AssertionError):
                raise

    return cast(StepFunctionType, wrapper)


register_type = behave.register_type


def given(pattern: str) -> StepFunctionWrapperType:
    return step_wrapper(behave.given, pattern)


def then(pattern: str) -> StepFunctionWrapperType:
    return step_wrapper(behave.then, pattern)


def when(pattern: str) -> StepFunctionWrapperType:
    return step_wrapper(behave.when, pattern)



__all__ = [
    'Context',
    'Feature',
    'Scenario',
    'Step',
    'Status',
    'Table',
    'Row',
    'register_type',
    'given',
    'then',
    'when',
]
