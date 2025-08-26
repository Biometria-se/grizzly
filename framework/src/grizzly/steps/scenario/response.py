"""Module contains step implementations that handles [Request][grizzly.tasks.request] task responses.

## Arguments

### `expression`

Valid for:

* [Save matches][grizzly.steps.scenario.response.step_response_save_matches]
* [Save][grizzly.steps.scenario.response.step_response_save]
* [Validate][grizzly.steps.scenario.response.step_response_validate]

| Name               | Type   | Description                                                                                                | Default |
| ------------------ | ------ | ---------------------------------------------------------------------------------------------------------- | ------- |
| `expected_matches` | `int`  | number of expected matches that `expression` should get, a value of `-1` means `Any` number of matches     | `1`     |
| `as_json`          | `bool` | always return matches as a JSON list, by default if there's a single match it will be returned as a string | `False` |

"""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

import parse
from grizzly_common.text import permutation
from grizzly_common.transformer import TransformerContentType

from grizzly.steps._helpers import add_request_response_status_codes, add_save_handler, add_validation_handler
from grizzly.tasks import RequestTask
from grizzly.tasks.clients import HttpClientTask
from grizzly.types import ResponseTarget
from grizzly.types.behave import Context, register_type, then, when

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.context import GrizzlyContext


@parse.with_pattern(r'is( not)?', regex_group_count=1)
@permutation(vector=(False, True))
def parse_condition(text: str) -> bool:
    return text is not None and text.strip() == 'is'


register_type(
    Condition=parse_condition,
    ResponseTarget=ResponseTarget.from_string,
    ContentType=TransformerContentType.from_string,
)


@then('save optional response {target:ResponseTarget} "{expression}" that matches "{match_with}" in variable "{variable}" with default value "{default_value}"')
def step_response_save_matches_optional(context: Context, target: ResponseTarget, expression: str, match_with: str, variable: str, default_value: str) -> None:
    """Save specified parts of a response, either from meta data (header) or payload (body), in a variable.

    With this step it is possible to change variable values and as such use values from a response later on in the load test.

    The [Request][grizzly.tasks.request] task preceded by this step will **not** fail if the specified `expression` has no or more than one match,
    the value of `variable` will be set to `default_value`.

    Example:
    ```gherkin
    # only token is matched and saved in TOKEN, by using regexp match groups
    And value for variable "TOKEN" is "none"
    Then save response metadata "$.Authentication" that matches "Bearer (.*)$" in variabel "TOKEN"

    # the whole value is saved, as long as Authentication starts with "Bearer"
    And value for variable "HEADER_AUTHENTICATION" is "none"
    Then save response metadata "$.Authentication" that matches "^Bearer .*$" in variable "HEADER_AUTHENTICATION"

    # only the numerical suffix is saved in the variable
    And value for variable "AtomicIntegerIncrementer.measurermentId" is "1"
    Then save response payload "$.measurement.id" that matches "^cpu([\\d]+)$" in "measurementId"

    # the whole value is saved, as long as the value starts with "cpu"
    And value for variable "measurementId" is "0"
    Then save response payload "$.measurement.id" that matches "^cpu[\\d]+$" in "measurementId"

    # xpath example
    And value for variable "xmlMeasurementId" is "none"
    Then save response payload "//measurement[0]/id/text() | content_type=xml" that matches "^cpu[\\d]+$" in "xmlMeasurementId"
    ```

    Args:
        target (ResponseTarget): `metadata` or `payload`, depending on which part of the response should be used
        expression (str): JSON path or XPath expression for finding the property
        match_with (str): static value or a regular expression
        variable (str): name of the already initialized variable to save the value in
        default_value (str): value to set if there is no match

    """
    add_save_handler(cast('GrizzlyContext', context.grizzly), target, expression, match_with, variable, default_value=default_value)


@then('save response {target:ResponseTarget} "{expression}" that matches "{match_with}" in variable "{variable}"')
def step_response_save_matches(context: Context, target: ResponseTarget, expression: str, match_with: str, variable: str) -> None:
    """Save specified parts of a response, either from meta data (header) or payload (body), in a variable.

    With this step it is possible to change variable values and as such use values from a response later on in the load test.

    The [Request][grizzly.tasks.request] task preceded by this step will fail if the specified `expression` has no or more than one match.

    Example:
    ```gherkin
    # only token is matched and saved in TOKEN, by using regexp match groups
    And value for variable "TOKEN" is "none"
    Then save response metadata "$.Authentication" that matches "Bearer (.*)$" in variabel "TOKEN"

    # the whole value is saved, as long as Authentication starts with "Bearer"
    And value for variable "HEADER_AUTHENTICATION" is "none"
    Then save response metadata "$.Authentication" that matches "^Bearer .*$" in variable "HEADER_AUTHENTICATION"

    # only the numerical suffix is saved in the variable
    And value for variable "AtomicIntegerIncrementer.measurermentId" is "1"
    Then save response payload "$.measurement.id" that matches "^cpu([\\d]+)$" in "measurementId"

    # the whole value is saved, as long as the value starts with "cpu"
    And value for variable "measurementId" is "0"
    Then save response payload "$.measurement.id" that matches "^cpu[\\d]+$" in "measurementId"

    # xpath example
    And value for variable "xmlMeasurementId" is "none"
    Then save response payload "//measurement[0]/id/text() | content_type=xml" that matches "^cpu[\\d]+$" in "xmlMeasurementId"
    ```

    Args:
        target (ResponseTarget): `metadata` or `payload`, depending on which part of the response should be used
        expression (str): JSON path or XPath expression for finding the property
        match_with (str): static value or a regular expression
        variable (str): name of the already initialized variable to save the value in

    """
    add_save_handler(cast('GrizzlyContext', context.grizzly), target, expression, match_with, variable, default_value=None)


@then('save optional response {target:ResponseTarget} "{expression}" in variable "{variable}" with default value "{default_value}"')
def step_response_save_optional(context: Context, target: ResponseTarget, expression: str, variable: str, default_value: str) -> None:
    """Save metadata (header) or payload (body) value from a response in a variable.

    This step expression is the same as [Save matches optional][grizzly.steps.scenario.response.step_response_save_matches_optional]
    if `match_with` is set to `.*`.

    With this step it is possible to change variable values and as such use values from a response later on in the load test.

    The [Request][grizzly.tasks.request] task preceded by this step will **not** fail if the specified `expression` has no or more than one match,
    the value of `variable` will be set to `default_value`.

    Example:
    ```gherkin
    Then save optional response metadata "$.Authentication" in variable "HEADER_AUTHENTICATION" with default value "none"

    Then save optional response payload "$.Result.ShipmentId" in variable "ShipmentId" with default value "SH1395865"

    Then save optional response payload "//measurement[0]/id/text()" in "xmlMeasurementId" with default value "foobar"
    ```

    Args:
        target (ResponseTarget): `metadata` or `payload`, depending on which part of the response should be used
        expression (str): JSON path or XPath expression for finding the property
        variable (str): name of the already initialized variable to save the value in
        default_value (str): value to set if there is no match

    """
    add_save_handler(cast('GrizzlyContext', context.grizzly), target, expression, '.*', variable, default_value=default_value)


@then('save response {target:ResponseTarget} "{expression}" in variable "{variable}"')
def step_response_save(context: Context, target: ResponseTarget, expression: str, variable: str) -> None:
    """Save metadata (header) or payload (body) value from a response in a variable.

    This step expression is the same as [Save matches][grizzly.steps.scenario.response.step_response_save_matches]
    if `match_with` is set to `.*`.

    With this step it is possible to change variable values and as such use values from a response later on in the load test.

    The [Request][grizzly.tasks.request] task preceded by this step will fail if the specified `expression` has no or more than one match.

    Example:
    ```gherkin
    Then save response metadata "$.Authentication" in variable "HEADER_AUTHENTICATION"

    Then save response payload "$.Result.ShipmentId" in variable "ShipmentId"

    Then save response payload "//measurement[0]/id/text()" in "xmlMeasurementId"
    ```

    Args:
        target (ResponseTarget): `metadata` or `payload`, depending on which part of the response should be used
        expression (str): JSON path or XPath expression for finding the property
        variable (str): name of the already initialized variable to save the value in

    """
    add_save_handler(cast('GrizzlyContext', context.grizzly), target, expression, '.*', variable, default_value=None)


@when('response {target:ResponseTarget} "{expression}" {condition:Condition} "{match_with}" fail request')
def step_response_validate(context: Context, target: ResponseTarget, expression: str, condition: bool, match_with: str) -> None:  # noqa: FBT001
    """Fail a request based on the value of a response meta data (header) or payload (body).

    Example:
    ```gherkin
    When any task fail restart scenario on failure
    When response metadata "$.['content-type']" is not ".*application/json.*" fail request
    When response metadata "$.['x-test-command']" is "abort" fail request
    When response metadata "$.Authentication" is not "Bearer .*$" fail request

    When any task fail stop user
    When response payload "$.measurement.id" is not "cpu[0-9]+" fail request
    When response payload "$.success" is "false" fail request
    When response payload "/root/measurement[@id="cpu"]/success/text()" is "'false'" fail request
    ```

    Args:
        target (ResponseTarget): `metadata` or `payload`, depending on which part of the response should be used
        expression (str): JSON path or XPath expression for finding the property
        condition (enum): "is" or "is not" depending on negative or postive matching
        match_with (str): static value or a regular expression

    """
    add_validation_handler(cast('GrizzlyContext', context.grizzly), target, expression, match_with, condition=condition)


@then('allow response status codes "{status_list}"')
def step_response_allow_status_codes(context: Context, status_list: str) -> None:
    """Set allowed response status codes for the latest defined request in the scenario.

    By default `200` is the only allowed respoonse status code. By prefixing a code with minus (`-`),
    it will be removed from the list of allowed response status codes.

    Example:
    ```gherkin
    Then get request with name "test-get-1" from endpoint "/api/test"
    And allow response status "200,302"

    Then get request with name "test-failed-get-2" from endpoint "/api/non-existing"
    And allow response status "-200,404"
    ```

    Args:
        status_list (str): comma separated list of integers

    """
    grizzly = cast('GrizzlyContext', context.grizzly)
    assert len(grizzly.scenario.tasks()) > 0, 'there are no requests in the scenario'

    request = grizzly.scenario.tasks()[-1]

    assert isinstance(request, RequestTask | HttpClientTask), 'previous task is not a request'

    add_request_response_status_codes(request, status_list)


@then('allow response status codes')
def step_response_allow_status_codes_table(context: Context) -> None:
    """Set allowed response status codes for the latest defined requests based on a data table.

    Specifies a comma separeated list of allowed return codes for the latest requests in a data table.

    By default `200` is the only allowed respoonse status code. By prefixing a code with minus (`-`),
    it will be removed from the list of allowed response status codes.

    Number of rows in the table specifies which of the latest defined requests the allowed response
    status codes should map to.

    The table **must** have the column header `status`.

    Example:
    ```gherkin
    Then get request with name "test-get-1" from endpoint "/api/test"
    Then get request with name "test-get-2" from endpoint "/api/test"
    And allow response status
     | status   |
     | 200, 302 |
     | 200,404  |
    ```

    Allowed response status codes for `test-get-1` is now `200` and `302`, and for `test-get-2` is
    now `200` and `404`.

    """
    assert context.table is not None, 'step table is missing'

    grizzly = cast('GrizzlyContext', context.grizzly)

    tasks = grizzly.scenario.tasks()
    number_of_requests = len(grizzly.scenario.tasks(RequestTask, HttpClientTask))

    assert number_of_requests > 0, 'there are no request tasks in the scenario'
    assert len(list(context.table)) <= number_of_requests, 'step table has more rows than there are request tasks'

    # last row = latest added request
    index = -1
    rows = list(reversed(list(context.table)))

    try:
        for row in rows:
            task = tasks[index]
            assert isinstance(task, RequestTask | HttpClientTask), f'task at index {index} is not a request'
            index -= 1
            add_request_response_status_codes(task, row['status'])
    except KeyError as e:
        message = 'step table does not have column "status"'
        raise AssertionError(message) from e


@then('set response content type to "{content_type:ContentType}"')
def step_response_content_type(context: Context, content_type: TransformerContentType) -> None:
    """Set the content type of a response, instead of guessing it.

    This is applicable when there is a `step_response_validate` or `step_response_save` is included in
    the scenario, and is valid only for the latest defined request.

    Example:
    ```gherkin
    And set response content type to "json"
    And set response content type to "application/json"
    And set response content type to "xml"
    And set response content type to "application/xml"
    And set response content type to "plain"
    And set response content type to "text/plain"
    ```

    Args:
        content_type (ContentType): expected content type of response

    """
    assert content_type != TransformerContentType.UNDEFINED, 'it is not allowed to set UNDEFINED with this step'

    grizzly = cast('GrizzlyContext', context.grizzly)
    assert len(grizzly.scenario.tasks()) > 0, 'there are no tasks in the scenario'

    request = grizzly.scenario.tasks()[-1]

    assert isinstance(request, RequestTask), 'latest task in scenario is not a request task'
    request.response.content_type = content_type
