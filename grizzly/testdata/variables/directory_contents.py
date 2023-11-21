"""@anchor pydoc:grizzly.testdata.variables.directory_contents Directory Contents
This variable provides a list of files in the specified directory.

## Format

Relative path of a directory under `requests/`.

## Arguments

* `repeat` _bool_ (optional) - wether values should be reused, e.g. when reaching the end it should start from the beginning again (default: `False`)
* `random` _bool_ (optional) - if files should be selected by random, instead of sequential from first to last (default: `False`)

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
from typing import Any, ClassVar, Dict, List, Optional, Type, cast

from grizzly.types import bool_type
from grizzly_extras.arguments import parse_arguments, split_value

from . import AtomicVariable


def atomicdirectorycontents__base_type__(value: str) -> str:
    """Validate values that `AtomicDirectoryContents` can be initialized with."""
    grizzly_context_requests = Path(environ.get('GRIZZLY_CONTEXT_ROOT', '')) / 'requests'
    if '|' in value:
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

    _files: Dict[str, List[str]]
    _settings: Dict[str, Dict[str, Any]]
    _requests_context_root: Path
    arguments: ClassVar[Dict[str, Any]] = {'repeat': bool_type, 'random': bool_type}

    def __init__(
        self,
        variable: str,
        value: str,
        *,
        outer_lock: bool = False,
    ) -> None:
        with self.semaphore(outer=outer_lock):
            safe_value = self.__class__.__base_type__(value)

            settings = {'repeat': False, 'random': False}

            if '|' in safe_value:
                directory, directory_arguments = split_value(safe_value)

                arguments = parse_arguments(directory_arguments)

                for argument, caster in self.__class__.arguments.items():
                    if argument in arguments:
                        settings[argument] = caster(arguments[argument])
            else:
                directory = safe_value

            super().__init__(variable, directory, outer_lock=True)

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
    def clear(cls: Type[AtomicDirectoryContents]) -> None:
        super().clear()

        instance = cast(AtomicDirectoryContents, cls.get())
        variables = list(instance._files.keys())

        for variable in variables:
            del instance._files[variable]
            del instance._settings[variable]

    def _create_file_queue(self, directory: str) -> List[str]:
        parent_part = len(str(self._requests_context_root)) + 1
        queue = [
            str(path)[parent_part:]
            for path in (self._requests_context_root / directory).rglob('*')
            if path.is_file()
        ]
        queue.sort()

        return queue

    def __getitem__(self, variable: str) -> Optional[str]:
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
