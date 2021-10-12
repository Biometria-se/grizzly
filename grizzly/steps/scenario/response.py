'''This module contains step implementations that handles request responses.'''
import parse

from typing import cast

from behave.runner import Context
from behave import register_type, when, then  # pylint: disable=no-name-in-module

from ...context import LocustContext, RequestContext, ResponseContentType, ResponseTarget
from ...utils import add_save_handler, add_validation_handler, add_request_context_response_status_codes


@parse.with_pattern(r'is( not)?', regex_group_count=1)
def parse_condition(text: str) -> bool:
    return text is not None and text.strip() == 'is'


@parse.with_pattern(r'(metadata|payload)')
def parse_response_target(text: str) -> ResponseTarget:
    text = text.strip()

    if text == 'metadata':
        return ResponseTarget.METADATA
    elif text == 'payload':
        return ResponseTarget.PAYLOAD
    else:
        raise ValueError(f'"{text}" is an unknown response target')


def parse_response_content_type(text: str) -> ResponseContentType:
    if text.strip() in ['application/json', 'json']:
        return ResponseContentType.JSON
    elif text.strip() in ['application/xml', 'xml']:
        return ResponseContentType.XML
    elif text.strip() in ['text/plain', 'plain']:
        return ResponseContentType.PLAIN
    else:
        raise ValueError(f'"{text}" is an unknown response content type')


register_type(
    Condition=parse_condition,
    ResponseTarget=parse_response_target,
    ResponseContentType=parse_response_content_type,
)


@then(u'save response {target:ResponseTarget} "{expression}" that matches "{match_with}" in variable "{variable}"')
def step_response_save_matches(context: Context, target: ResponseTarget, expression: str, match_with: str, variable: str) -> None:
    '''Save specified parts of a response, either from meta data (header) or payload (body), in a variable.

    With this step it is possible to change variable values and as such use values from a response later on in the load test.

    This step will fail if the specified `expression` has no match or more than one match.

    ```gherkin
    # only token is matched and saved in TOKEN, by using regexp match groups
    And value of variable "TOKEN" is "none"
    Then save response metadata "$.Authentication" that matches "Bearer (.*)$" in variabel "TOKEN"

    # the whole value is saved, as long as Authentication starts with "Bearer"
    And value of variable "HEADER_AUTHENTICATION" is "none"
    Then save response metadata "$.Authentication" that matches "^Bearer .*$" in variable "HEADER_AUTHENTICATION"

    # only the numerical suffix is saved in the variable
    And value of variable "AtomicInteger.measurermentId" is "0"
    Then save response payload "$.measurement.id" that matches "^cpu([\\d]+)$" in "AtomicInteger.measurementId"

    # the whole value is saved, as long as the value starts with "cpu"
    And value of variable "measurementId" is "0"
    Then save response payload "$.measurement.id" that matches "^cpu[\\d]+$" in "measurementId"

    # xpath example
    And set response content type to "application/xml"
    And value of variable "xmlMeasurementId" is "none"
    Then save response payload "//measurement[0]/id/text()" that matches "^cpu[\\d]+$" in "xmlMeasurementId"
    ```

    Args:
        target (enum): "metadata" or "payload", depending on which part of the response should be used
        expression (str): JSON path or XPath expression for finding the property
        match_with (str): static value or a regular expression
        variable (str): name of the already initialized variable to save the value in
    '''
    add_save_handler(cast(LocustContext, context.locust), target, expression, match_with, variable)


@then(u'save response {target:ResponseTarget} "{expression}" in variable "{variable}"')
def step_response_save(context: Context, target: ResponseTarget, expression: str, variable: str) -> None:
    '''Save metadata (header) or payload (body) value from a response in a variable.

    This step is the same as `step_response_save_matches` if `match_with` is set to `.*`.

    With this step it is possible to change variable values and as such use values from a response later on in the load test.

    This step will fail if the specified `expression` has no match or more than one match.

    ```gherkin
    Then save response metadata "$.Authentication" in variable "HEADER_AUTHENTICATION"

    Then save response payload "$.Result.ShipmentId" in variable "ShipmentId"

    Then save response payload "//measurement[0]/id/text()" in "xmlMeasurementId"
    ```

    Args:
        target (enum): "metadata" or "payload", depending on which part of the response should be used
        expression (str): JSON path or XPath expression for finding the property
        variable (str): name of the already initialized variable to save the value in
    '''
    add_save_handler(cast(LocustContext, context.locust), target, expression, '.*', variable)


@when(u'response {target:ResponseTarget} "{expression}" {condition:Condition} "{match_with}" stop user')
def step_response_validate(context: Context, target: ResponseTarget, expression: str, condition: bool, match_with: str) -> None:
    '''Fails the scenario based on the value of a response meta data (header) or payload (body).

    ```gherkin
    When response metadata "$.['content-type']" is not ".*application/json.*" stop user
    When response metadata "$.['x-test-command']" is "abort" stop user
    When response metadata "$.Authentication" is not "Bearer .*$" stop user

    When response payload "$.measurement.id" is not "cpu[0-9]+" stop user
    When response payload "$.success" is "false" stop user
    When response payload "/root/measurement[@id="cpu"]/success/text()" is "'false'" stop user
    ```

    Args:
        target (enum): "metadata" or "payload", depending on which part of the response should be used
        expression (str): JSON path or XPath expression for finding the property
        condition (enum): "is" or "is not" depending on negative or postive matching
        match_with (str): static value or a regular expression
    '''
    add_validation_handler(cast(LocustContext, context.locust), target, expression, match_with, condition)


@then(u'allow response status codes "{status_list}"')
def step_response_allow_status_codes(context: Context, status_list: str) -> None:
    '''Set allowed response status codes for the latest defined request in the scenario.

    By default `200` is the only allowed respoonse status code. By prefixing a code with minus (`-`),
    it will be removed from the list of allowed response status codes.

    ```gherkin
    Then get request with name "test-get-1" from endpoint "/api/test"
    And allow response status "200,302"

    Then get request with name "test-failed-get-2" from endpoint "/api/non-existing"
    And allow response status "-200,404"
    ```

    Args:
        status_list (str): comma separated list of integers
    '''
    context_locust = cast(LocustContext, context.locust)
    assert len(context_locust.scenario.tasks) > 0, 'There are no requests in the scenario'

    request = context_locust.scenario.tasks[-1]

    assert isinstance(request, RequestContext), f'Previous task is not a request'

    add_request_context_response_status_codes(request, status_list)


@then(u'allow response status codes')
def step_response_allow_status_codes_table(context: Context) -> None:
    '''Set allowed response status codes for the latest defined requests based on a data table.

    Specifies a comma separeated list of allowed return codes for the latest requests in a data table.

    By default `200` is the only allowed respoonse status code. By prefixing a code with minus (`-`),
    it will be removed from the list of allowed response status codes.

    Number of rows in the table specifies which of the latest defined requests the allowed response
    status codes should map to.

    The table **must** have the column header `status`.

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
    '''
    assert context.table is not None, f'Step data table is mandatory'

    context_locust = cast(LocustContext, context.locust)

    number_of_requests = len(context_locust.scenario.tasks)

    assert number_of_requests > 0, 'There are no requests in the scenario'
    assert len(list(context.table)) <= len(context_locust.scenario.tasks), 'Data table has more rows than there are requests'

    # last row = latest added request
    index = -1
    rows = list(reversed(list(context.table)))

    assert len(rows) <= number_of_requests, 'There are more rows in the table than added requests'

    for row in rows:
        try:
            request = context_locust.scenario.tasks[index]
            assert isinstance(request, RequestContext), f'Task at index {index} is not a request'
            index -= 1
            add_request_context_response_status_codes(request, row['status'])
        except KeyError:
            raise AssertionError('data table does not have column "status"')


@then(u'set response content type to "{content_type:ResponseContentType}"')
def step_response_content_type(context: Context, content_type: ResponseContentType) -> None:
    '''Set the content type of a response, instead of guessing it.

    This is applicable when there is a `step_response_validate` or `step_response_save` is included in
    the scenario, and is valid only for the latest defined request.

    ```gherkin
    And set response content type to "json"
    And set response content type to "application/json"
    And set response content type to "xml"
    And set response content type to "application/xml"
    And set response content type to "plain"
    And set response content type to "text/plain"
    ```

    Args:
        content_type (ResponseContentType): expected content type of response
    '''

    assert content_type != ResponseContentType.GUESS, f'It is now allowed to set GUESS with this step'

    context_locust = cast(LocustContext, context.locust)
    assert len(context_locust.scenario.tasks) > 0, f'There are no requests in the scenario'

    request = context_locust.scenario.tasks[-1]

    assert isinstance(request, RequestContext), f'Latest task in scenario is not a request'
    request.response.content_type = content_type
