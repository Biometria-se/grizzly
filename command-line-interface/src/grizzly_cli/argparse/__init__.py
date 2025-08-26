"""Grizzly wrapper around argparse."""

from __future__ import annotations

import re
import sys
from argparse import ArgumentParser as CoreArgumentParser
from argparse import Namespace, _SubParsersAction
from typing import TYPE_CHECKING, Any, cast

from grizzly_cli.argparse.bashcompletion import BashCompletionAction
from grizzly_cli.argparse.bashcompletion import hook as bashcompletion_hook
from grizzly_cli.argparse.markdown import MarkdownFormatter, MarkdownHelpAction

if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Sequence
    from typing import IO

    from _typeshed import SupportsWrite

ArgumentSubParser = _SubParsersAction


class ArgumentParser(CoreArgumentParser):
    def __init__(self, *args: Any, markdown_help: bool = False, bash_completion: bool = False, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)

        self.markdown_help = markdown_help
        self.bash_completion = bash_completion

        if self.markdown_help:
            self.add_argument('--md-help', action=MarkdownHelpAction)

        if self.bash_completion:
            self.add_argument('--bash-completion', action=BashCompletionAction)

        self._optionals.title = 'optional arguments'

    def error_no_help(self, message: str) -> None:
        sys.stderr.write(f'{self.prog}: error: {message}\n')
        sys.exit(2)

    def print_help(self, file: SupportsWrite[str] | None = None) -> None:
        """Make help more command line friendly, if there is markdown markers in the text."""
        file = cast('IO[str] | None', file)
        if not self.markdown_help:
            super().print_help(file)
            return

        if self.formatter_class is not MarkdownFormatter:
            original_description = self.description
            original_actions = self._actions

            # code block "markers" are not really nice to have in cli help
            if self.description is not None:
                self.description = '\n'.join([line for line in self.description.split('\n') if '```' not in line])
                self.description = self.description.replace('\n\n', '\n')

            for action in self._actions:
                if action.help is not None:
                    # remove any markdown link markers
                    action.help = re.sub(r'\[([^\]]*)\][\(\[][^\)]*[\)\]]', r'\1', action.help)

        super().print_help(file)

        if self.formatter_class is not MarkdownFormatter:
            self.description = original_description
            self._actions = original_actions

    def parse_args(self, args: Sequence[str] | None = None, namespace: Namespace | None = None) -> Namespace:  # type: ignore[override]
        """Add `--bash-complete` to all parsers, if enabled for parser."""
        if self.bash_completion:
            bashcompletion_hook(self)

        return super().parse_args(args, namespace)
