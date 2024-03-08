"""@anchor pydoc:grizzly.tasks.write_file Write file
This task writes contents to a file on disk.

If the specified file already exist, new content will be appended to the existing file.

## Step implementations

* {@pylink grizzly.steps.scenario.tasks.write_file.step_task_write_file}

## Arguments

* `file_name` _str_ - file name relative to `<context root>/requests`, can contain directory levels
"""
from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

from . import GrizzlyTask, grizzlytask, template

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.scenarios import GrizzlyScenario


@template('file_name', 'content')
class WriteFileTask(GrizzlyTask):
    content: str
    file_name: str

    def __init__(self, file_name: str, content: str) -> None:
        super().__init__()

        self.file_name = file_name
        self.content = content

    def __call__(self) -> grizzlytask:
        @grizzlytask
        def task(parent: GrizzlyScenario) -> Any:
            file_name = parent.render(self.file_name)
            file = Path(self._context_root) / 'requests' / file_name

            response_length = 0

            try:
                content = parent.render(self.content)

                file.parent.mkdir(parents=True, exist_ok=True)

                with file.open('a+') as fd:
                    response_length = fd.write(f'{content}\n')
            except Exception as exception:
                parent.user.environment.events.request.fire(
                    request_type='FWRT',
                    name=f'{parent.user._scenario.identifier} FileWriteTask=>{file_name}',
                    response_time=0,
                    response_length=response_length,
                    context=parent.user._context,
                    exception=exception,
                )

        return task
