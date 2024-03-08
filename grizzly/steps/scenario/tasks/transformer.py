"""@anchor pydoc:grizzly.steps.scenario.tasks.transformer Transformer
This module contains step implementations for the {@pylink grizzly.tasks.transformer} task.
"""
from __future__ import annotations

from typing import cast

from grizzly.context import GrizzlyContext
from grizzly.tasks import TransformerTask
from grizzly.types.behave import Context, register_type, then
from grizzly_extras.transformer import TransformerContentType

register_type(
    ContentType=TransformerContentType.from_string,
)


@then('parse "{content}" as "{content_type:ContentType}" and save value of "{expression}" in variable "{variable}"')
def step_task_transform(context: Context, content: str, content_type: TransformerContentType, expression: str, variable: str) -> None:
    """Create an instance of the {@pylink grizzly.tasks.transformer} task.

    Transforms the specified `content` with `content_type` to an object that an transformer can extract information from
    with the specified `expression`.

    See {@pylink grizzly.tasks.transformer} task documentation for more information about the task.

    Example:
    ```gherkin
    And value for variable "document_id" is "None"
    And value for variable "document_title" is "None"
    And value for variable "document" is "{\"document\": {\"id\": \"DOCUMENT_8843-1\", \"title\": \"TPM Report 2021\"}}"
    ...
    Then parse "{{ document }}" as "json" and save value of "$.document.id" in variable "document_id"
    Then parse "{{ document }}" as "json" and save value of "$.document.title" in variable "document_title"
    ```

    Args:
        contents (str): contents to parse, supports {@link framework.usage.variables.templating} or a static string
        content_type (TransformerContentType): MIME type of `contents`
        expression (str): JSON or XPath expression for specific value in `contents`
        variable (str): name of variable to save value to, must have been initialized

    """
    grizzly = cast(GrizzlyContext, context.grizzly)
    grizzly.scenario.tasks.add(TransformerTask(
        content=content,
        content_type=content_type,
        expression=expression,
        variable=variable,
    ))
