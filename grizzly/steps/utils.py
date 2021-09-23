'''This package contains steps that can be useful during development or troubleshooting of a
feature file, but should not be included in a finished, testable, feature.
'''

from behave.runner import Context
from behave import then  # pylint: disable=no-name-in-module

@then(u'fail')
def step_utils_fail(context: Context) -> None:
    '''Force a failed scenario. Can be useful when writing a new scenario.

    ```gherkin
    Then fail
    ```
    '''
    assert 0
