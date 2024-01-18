"""@anchor pydoc:grizzly.steps.utils Utils
This module contains steps that can be useful during development or troubleshooting of a
feature file, but should not be included in a finished, testable, feature.
"""
from __future__ import annotations

from typing import Any

from grizzly.types.behave import Context, then


@then('fail')
def step_utils_fail(context: Context, *_args: Any, **_kwargs: Any) -> None:  # noqa: ARG001
    """Force a failed scenario. Can be useful when writing a new scenario. The scenario will fail before `locust` has started, so only when
    the scenario is setup.

    ```gherkin
    Then fail
    ```
    """
    message = 'manually failed'
    raise AssertionError(message)
