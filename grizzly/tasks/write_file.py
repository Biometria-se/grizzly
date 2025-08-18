"""Task writes contents to a file on disk.

If the specified file already exist, new content will be appended to the existing file.

## Step implementations

* [Create or append][grizzly.steps.scenario.tasks.write_file.step_task_write_file_create_or_append]

* [Temporary][grizzly.steps.scenario.tasks.write_file.step_task_write_file_temporary]

"""

from __future__ import annotations

from base64 import b64decode
from contextlib import suppress
from pathlib import Path
from typing import TYPE_CHECKING, Any

from grizzly.testdata.utils import resolve_parameters
from grizzly.utils import has_parameter, has_template

from . import GrizzlyTask, grizzlytask, template

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.scenarios import GrizzlyScenario


@template('file_name', 'content')
class WriteFileTask(GrizzlyTask):
    content: str
    file_name: str
    file: Path | None = None
    temp_file: bool

    def __init__(self, file_name: str, content: str, *, temp_file: bool = False) -> None:
        super().__init__(timeout=None)

        self.file_name = file_name
        self.content = content
        self.temp_file = temp_file

        # check if content is stored in parameters
        if has_parameter(self.content):
            self.content = resolve_parameters(self.grizzly.scenario, self.content)

        # always base64 decode, since it could be base64 encoded
        with suppress(Exception):
            self.content = b64decode(self.content).decode()

    def __call__(self) -> grizzlytask:
        @grizzlytask
        def task(parent: GrizzlyScenario) -> Any:
            file_name = parent.user.render(self.file_name)
            file = Path(self._context_root) / 'requests' / file_name

            # file has already been created
            if self.temp_file and self.file is not None and file.exists():
                return

            self.file = file

            response_length = 0

            try:
                content = parent.user.render(self.content)

                # sub render variable content
                if has_template(content):
                    content = parent.user.render(content)

                if has_parameter(content):
                    content = resolve_parameters(parent.user._scenario, content)

                self.file.parent.mkdir(parents=True, exist_ok=True)

                mode = 'a+' if not self.temp_file else 'w+'

                line_break = '\n' if mode == 'a+' else ''

                with self.file.open(mode) as fd:
                    response_length = fd.write(f'{content}{line_break}')
            except Exception as exception:
                parent.user.environment.events.request.fire(
                    request_type='FWRT',
                    name=f'{parent.user._scenario.identifier} FileWriteTask=>{file_name}',
                    response_time=0,
                    response_length=response_length,
                    context=parent.user._context,
                    exception=exception,
                )

        @task.on_stop
        def on_stop(parent: GrizzlyScenario) -> None:  # noqa: ARG001
            if self.file is not None and self.temp_file:
                self.file.unlink(missing_ok=True)

        return task
