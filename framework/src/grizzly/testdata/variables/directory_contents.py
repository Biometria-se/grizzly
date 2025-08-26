"""Provide a list of files in the specified directory.

## Format

Relative path of a directory under `requests/`.

## Arguments

| Name     | Type   | Description                                                                                          | Default |
| -------- | ------ | ---------------------------------------------------------------------------------------------------- | ------- |
| `repeat` | `bool` | whether values should be reused, e.g. when reaching the end it should start from the beginning again | `False` |
| `random` | `bool` | if files should be selected by random, instead of sequential from first to last                      | `False` |

## Example

With the following directory structure:

```plain
.
└── requests
    └── files
        ├── file1.bin
        ├── file2.bin
        ├── file3.bin
        ├── file4.bin
        └── file5.bin
```

```gherkin
And value for variable "AtomicDirectoryContents.files" is "files/ | repeat=True, random=False"
And put request "{{ AtomicDirectoryContents.files }}" with name "put-file" to endpoint "/tmp"
```

First request will provide `file1.bin`, second `file2.bin` etc.
"""

from __future__ import annotations

from contextlib import suppress
from os import environ
from pathlib import Path
from secrets import randbelow
from typing import TYPE_CHECKING, ClassVar, cast

from grizzly_common.arguments import parse_arguments, split_value
from grizzly_common.text import has_separator

from grizzly.types import StrDict, bool_type

from . import AtomicVariable

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.context import GrizzlyContextScenario


def atomicdirectorycontents__base_type__(value: str) -> str:
    grizzly_context_requests = Path(environ.get('GRIZZLY_CONTEXT_ROOT', '')) / 'requests'
    if has_separator('|', value):
        [directory_value, directory_arguments] = split_value(value)

        try:
            arguments = parse_arguments(directory_arguments)
        except ValueError as e:
            message = f'AtomicDirectoryContents: {e!s}'
            raise ValueError(message) from e

        for argument_name, argument_value in arguments.items():
            if argument_name not in AtomicDirectoryContents.arguments:
                message = f'AtomicDirectoryContents: argument {argument_name} is not allowed'
                raise ValueError(message)

            AtomicDirectoryContents.arguments[argument_name](argument_value)

        value = f'{directory_value} | {directory_arguments}'
    else:
        directory_value = value

    path = grizzly_context_requests / directory_value

    if not path.is_dir():
        message = f'AtomicDirectoryContents: {directory_value} is not a directory in {grizzly_context_requests!s}'
        raise ValueError(message)

    return value


class AtomicDirectoryContents(AtomicVariable[str]):
    __base_type__ = atomicdirectorycontents__base_type__
    __initialized: bool = False

    _files: dict[str, list[str]]
    _settings: dict[str, StrDict]
    _requests_context_root: Path
    arguments: ClassVar[StrDict] = {'repeat': bool_type, 'random': bool_type}

    def __init__(
        self,
        *,
        scenario: GrizzlyContextScenario,
        variable: str,
        value: str,
        outer_lock: bool = False,
    ) -> None:
        with self.semaphore(outer=outer_lock):
            safe_value = self.__class__.__base_type__(value)

            settings = {'repeat': False, 'random': False}

            if has_separator('|', safe_value):
                directory, directory_arguments = split_value(safe_value)

                arguments = parse_arguments(directory_arguments)

                for argument, caster in self.__class__.arguments.items():
                    if argument in arguments:
                        settings[argument] = caster(arguments[argument])
            else:
                directory = safe_value

            super().__init__(scenario=scenario, variable=variable, value=directory, outer_lock=True)

            if self.__initialized:
                if variable not in self._files:
                    self._files[variable] = self._create_file_queue(directory)

                if variable not in self._settings:
                    self._settings[variable] = settings

                return

            self._requests_context_root = Path(environ.get('GRIZZLY_CONTEXT_ROOT', '.')) / 'requests'
            self._files = {variable: self._create_file_queue(directory)}
            self._settings = {variable: settings}
            self.__initialized = True

    @classmethod
    def clear(cls: type[AtomicDirectoryContents]) -> None:
        super().clear()

        instances = cls._instances.get(cls, {})
        for scenario in instances:
            instance = cast('AtomicDirectoryContents', cls.get(scenario))
            variables = list(instance._files.keys())

            for variable in variables:
                del instance._files[variable]
                del instance._settings[variable]

    def _create_file_queue(self, directory: str) -> list[str]:
        parent_part = len(str(self._requests_context_root)) + 1
        queue = [str(path)[parent_part:] for path in (self._requests_context_root / directory).rglob('*') if path.is_file()]
        queue.sort()

        return queue

    def __getitem__(self, variable: str) -> str | None:
        with self.semaphore():
            self._get_value(variable)

            try:
                settings = self._settings[variable]

                if settings['random'] is True:
                    roof = len(self._files[variable])
                    index = randbelow(roof)
                else:
                    index = 0

                value = self._files[variable].pop(index)

                if settings['repeat'] is True:
                    self._files[variable].append(value)
            except (IndexError, ValueError):
                return None
            else:
                return value

    def __delitem__(self, variable: str) -> None:
        with self.semaphore():
            with suppress(KeyError):
                del self._files[variable]

            with suppress(KeyError):
                del self._settings[variable]

            super().__delitem__(variable)
