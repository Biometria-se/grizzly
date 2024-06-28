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
        content (str): what to write in the file, can be base64 encoded
        file_name (str): file name, which can include non-existing directory levels (will be created)

    """
    grizzly = cast(GrizzlyContext, context.grizzly)
    grizzly.scenario.tasks.add(WriteFileTask(file_name=file_name, content=content))


@then('write "{content}" in temporary file "{file_name}"')
def step_task_write_temp_file(context: Context, content: str, file_name: str) -> None:
    """Create an instance of the {@pylink grizzly.tasks.write_file} task, which will remove the file when test is stopped.

    Writes specified content, as-is, in the specified file (no line break added), the file will be removed when the test stops.
    The file will be created in the first iteration, and then be a no-op task for any following iterations.

    Both content and file name support templating, and content can be base64 encoded.

    See {@pylink grizzly.tasks.write_file} task documentation for more information about the task.

    Example:
    ```gherkin
    Then write "$env::BASE64_ENCODED_BINARY_FILE" in temporary file "certificate.bin"
    ```

    Args:
        content (str): what to write in the file, can be base64 encoded
        file_name (str): file name, which can include non-existing directory levels (will be created)

    """
    grizzly = cast(GrizzlyContext, context.grizzly)
    grizzly.scenario.tasks.add(WriteFileTask(file_name=file_name, content=content, temp_file=True))
