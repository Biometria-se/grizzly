"""Module contains steps that can be useful during development or troubleshooting of a
feature file, but should not be included in a finished, testable, feature.
"""
from __future__ import annotations

from grizzly.types.behave import Context, then


@then('fail')
def step_utils_fail(_context: Context) -> None:
    """Force a failed scenario. Can be useful when writing a new scenario. The scenario will fail before `locust` has started, so only when
    the scenario is setup.

    ```gherkin
    Then fail
    ```
    """
    assert 0  # noqa: PT015
