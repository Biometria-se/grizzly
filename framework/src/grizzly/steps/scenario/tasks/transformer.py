"""Module contains step implementations for the [Transformer][grizzly.tasks.transformer] task."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

from grizzly_common.transformer import TransformerContentType

from grizzly.tasks import TransformerTask
from grizzly.types.behave import Context, register_type, then

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.context import GrizzlyContext

register_type(
    ContentType=TransformerContentType.from_string,
)


@then('parse "{content}" as "{content_type:ContentType}" and save value of "{expression}" in variable "{variable}"')
def step_task_transformer_parse(context: Context, content: str, content_type: TransformerContentType, expression: str, variable: str) -> None:
    """Create an instance of the [Transformer][grizzly.tasks.transformer] task.

    Transforms the specified `content` with `content_type` to an object that an transformer can extract information from
    with the specified `expression`.

    See [Transformer][grizzly.tasks.transformer] task documentation for more information.

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
        content (str): contents to parse, supports [templating][framework.usage.variables.templating] or a static string
        content_type (TransformerContentType): MIME type of `contents`
        expression (str): JSON or XPath expression for specific value in `contents`
        variable (str): name of variable to save value to, must have been initialized

    """
    grizzly = cast('GrizzlyContext', context.grizzly)
    grizzly.scenario.tasks.add(
        TransformerTask(
            content=content,
            content_type=content_type,
            expression=expression,
            variable=variable,
        ),
    )
