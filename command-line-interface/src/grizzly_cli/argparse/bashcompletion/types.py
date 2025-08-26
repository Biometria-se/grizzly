"""Types for grizzly argparse bashcompletion logic."""

from __future__ import annotations

import sys
from argparse import ArgumentTypeError
from fnmatch import filter as fnmatch_filter
from os.path import sep as path_sep
from pathlib import Path
from typing import cast

__all__ = [
    'BashCompletionTypes',
]

ESCAPE_CHARACTERS: dict[str, str | int | None] = {
    ' ': '\\ ',
    '(': '\\(',
    ')': '\\)',
}


class BashCompletionTypes:
    class File:
        def __init__(self, *args: str, missing_ok: bool = False) -> None:
            self.patterns = list(args)
            self.cwd = Path.cwd()
            self.missing_ok = missing_ok

        def __call__(self, value: str) -> str:
            if self.missing_ok:
                return value

            file = Path(value)

            if not file.exists():
                message = f'{value} does not exist'
                raise ArgumentTypeError(message)

            if not file.is_file():
                message = f'{value} is not a file'
                raise ArgumentTypeError(message)

            matches = [match for pattern in self.patterns for match in fnmatch_filter([value], pattern)]

            if len(matches) < 1:
                message = f'{value} does not match {", ".join(self.patterns)}'
                raise ArgumentTypeError(message)

            return value

        @classmethod
        def _transform_path(cls, value: str) -> str:
            value = value.translate(str.maketrans(ESCAPE_CHARACTERS))
            if sys.platform == 'win32':
                value = value.replace('/', path_sep)

            return value

        def list_files(self, value: str | None) -> dict[str, str]:
            matches: dict[str, str] = {}

            if value is not None:
                if sys.platform == 'win32':
                    value = value.replace(path_sep, '/')  # posix style

                for chr_with, chr_replace in ESCAPE_CHARACTERS.items():
                    value = value.replace(cast('str', chr_replace), chr_with)

            for pattern in self.patterns:
                for path in self.cwd.rglob(f'**/{pattern}'):
                    try:
                        path_match = path.relative_to(self.cwd)
                    except ValueError:
                        path_match = path

                    path_match_value = path_match.as_posix()

                    # skip any paths where any part is hidden, or any path that is not (partially) relative to provided value
                    if any(part.startswith('.') for part in path_match.parts) or (value is not None and not path_match_value.startswith(value)):
                        continue

                    match: dict[str, str] | None = None

                    # all paths are treated in posix style
                    if '/' in path_match_value:  # there is a directory in the match
                        try:
                            """
                            find first part that matches with provided value;
                            value = `hel`
                            path_match_value = `hello/example.txt`
                            should be `hello`, and a dir(ectory)
                            """
                            index_match = len(value or '')
                            index_sep = path_match_value.index('/', index_match)
                            match = {self._transform_path(path_match_value[:index_sep]): 'dir'}
                        except ValueError:
                            # no match against provided value, so assume file
                            pass

                    if match is None:
                        match = {self._transform_path(path_match_value): 'file'}

                    matches.update(match)

            return matches
