'''
@anchor pydoc:grizzly.steps.background.utils Utils
This package contains steps that can be useful during development or troubleshooting of a
feature file, but should not be included in a finished, testable, feature.
'''

from grizzly.types.behave import Context, then


@then(u'fail')
def step_utils_fail(context: Context) -> None:
    '''Force a failed scenario. Can be useful when writing a new scenario.

    ``` gherkin
    Then fail
    ```
    '''
    assert 0
