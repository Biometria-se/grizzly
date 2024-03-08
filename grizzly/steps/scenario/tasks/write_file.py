"""@anchor pydoc:grizzly.steps.scenario.tasks.write_file Write file
This module contains step implementations for the {@pylink grizzly.tasks.write_file} task.
"""
from __future__ import annotations

from typing import cast

from grizzly.context import GrizzlyContext
from grizzly.tasks import WriteFileTask
from grizzly.types.behave import Context, then


@then('write "{content}" in file "{file_name}"')
def step_task_write_file(context: Context, content: str, file_name: str) -> None:
    """Create an instance of the {@pylink grizzly.tasks.write_file} task.

    Writes content into specified file (adds new line after), if the file already exist the content will be appended.
    Useful for traceability or observability during a test.

    Both content and file name support templating.

    See {@pylink grizzly.tasks.write_file} task documentation for more information about the task.

    Example:
    ```gherkin
    Then write "{{ payload }}" in file "debug/request_response.log"
    Then write "hello world!" in file "debug/request_response.log"
    ```

    Args:
        content (str): what to write in the file
        file_name (str): file name, which can include non-existing directory levels (will be created)

    """
    grizzly = cast(GrizzlyContext, context.grizzly)
    grizzly.scenario.tasks.add(WriteFileTask(file_name=file_name, content=content))
