'''
@anchor pydoc:grizzly.testdata.variables.directory_contents Directory Contents
This variable provides a list of files in the specified directory.

## Format

Relative path of a directory under `requests/`.

## Arguments

* `repeat` _bool_ (optional) - wether values should be reused, e.g. when reaching the end it should start from the beginning again (default: `False`)
* `random` _bool_ (optional) - if files should be selected by random, instead of sequential from first to last (default: `False`)

## Example

With the following directory structure:

``` plain
.
└── requests
    └── files
        ├── file1.bin
        ├── file2.bin
        ├── file3.bin
        ├── file4.bin
        └── file5.bin
```

``` gherkin
And value for variable "AtomicDirectoryContents.files" is "files/ | repeat=True, random=False"
And put request "{{ AtomicDirectoryContents.files }}" with name "put-file" to endpoint "/tmp"
```

First request will provide `file1.bin`, second `file2.bin` etc.
'''
import os

from typing import Dict, List, Any, Type, Optional, cast
from pathlib import Path
from random import randint

from grizzly_extras.arguments import split_value, parse_arguments

from ...types import bool_type
from . import AtomicVariable


def atomicdirectorycontents__base_type__(value: str) -> str:
    grizzly_context_requests = os.path.join(os.environ.get('GRIZZLY_CONTEXT_ROOT', ''), 'requests')
    if '|' in value:
        [directory_value, directory_arguments] = split_value(value)

        try:
            arguments = parse_arguments(directory_arguments)
        except ValueError as e:
            raise ValueError(f'AtomicDirectoryContents: {str(e)}') from e

        for argument_name, argument_value in arguments.items():
            if argument_name not in AtomicDirectoryContents.arguments:
                raise ValueError(f'AtomicDirectoryContents: argument {argument_name} is not allowed')
            else:
                AtomicDirectoryContents.arguments[argument_name](argument_value)

        value = f'{directory_value} | {directory_arguments}'
    else:
        directory_value = value

    path = os.path.join(grizzly_context_requests, directory_value)

    if not os.path.isdir(path):
        raise ValueError(f'AtomicDirectoryContents: {directory_value} is not a directory in {grizzly_context_requests}')

    return value


class AtomicDirectoryContents(AtomicVariable[str]):
    __base_type__ = atomicdirectorycontents__base_type__
    __initialized: bool = False

    _files: Dict[str, List[str]]
    _settings: Dict[str, Dict[str, Any]]
    _requests_context_root: str
    arguments: Dict[str, Any] = {'repeat': bool_type, 'random': bool_type}

    def __init__(
        self,
        variable: str,
        value: str,
    ):
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

        super().__init__(variable, directory)

        with self._semaphore:
            if self.__initialized:
                if variable not in self._files:
                    self._files[variable] = self._create_file_queue(directory)

                if variable not in self._settings:
                    self._settings[variable] = settings

                return

            self._requests_context_root = os.path.join(os.environ.get('GRIZZLY_CONTEXT_ROOT', '.'), 'requests')
            self._files = {variable: self._create_file_queue(directory)}
            self._settings = {variable: settings}
            self.__initialized = True

    @classmethod
    def clear(cls: Type['AtomicDirectoryContents']) -> None:
        super().clear()

        instance = cast(AtomicDirectoryContents, cls.get())
        variables = list(instance._files.keys())

        for variable in variables:
            del instance._files[variable]
            del instance._settings[variable]

    def _create_file_queue(self, directory: str) -> List[str]:
        parent_part = len(self._requests_context_root) + 1
        queue = [
            str(path)[parent_part:]
            for path in Path(os.path.join(self._requests_context_root, directory)).rglob('*')
            if path.is_file()
        ]
        queue.sort()

        return queue

    def __getitem__(self, variable: str) -> Optional[str]:
        with self._semaphore:
            self._get_value(variable)

            try:
                settings = self._settings[variable]

                if settings['random'] is True:
                    roof = len(self._files[variable]) - 1
                    index = randint(0, roof)
                else:
                    index = 0

                value = self._files[variable].pop(index)

                if settings['repeat'] is True:
                    self._files[variable].append(value)

                return value
            except (IndexError, ValueError):
                return None

    # not possible to override already initialized variable
    def __setitem__(self, variable: str, value: Optional[str]) -> None:
        pass

    def __delitem__(self, variable: str) -> None:
        with self._semaphore:
            try:
                del self._files[variable]
            except KeyError:
                pass

            try:
                del self._settings[variable]
            except KeyError:
                pass

        super().__delitem__(variable)
