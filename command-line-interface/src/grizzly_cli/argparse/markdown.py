"""Functionality for getting argparse help in markdown."""

from __future__ import annotations

from argparse import SUPPRESS, Action, ArgumentParser, HelpFormatter, Namespace
from textwrap import fill as textwrap_fill
from types import MethodType
from typing import TYPE_CHECKING, Any, Union, cast

if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Callable, Iterable, Sequence

    from typing_extensions import Self


__all__ = [
    'MarkdownFormatter',
    'MarkdownHelpAction',
]


class MarkdownHelpAction(Action):
    def __init__(
        self,
        option_strings: list[str],
        dest: str = SUPPRESS,
        default: str = SUPPRESS,
        help: str = SUPPRESS,  # noqa: A002
        **kwargs: Any,
    ) -> None:
        super().__init__(
            option_strings=option_strings,
            dest=dest,
            default=default,
            help=help,
            nargs=0,
            **kwargs,
        )

    def __call__(
        self,
        parser: ArgumentParser,
        namespace: Namespace,  # noqa: ARG002
        values: Union[str, Sequence[Any], None],  # noqa: ARG002
        option_string: str | None = None,  # noqa: ARG002
    ) -> None:
        self.print_help(parser)

        parser.exit()

    def print_help(self, parser: ArgumentParser, level: int = 0) -> None:
        def format_help_markdown(self: ArgumentParser) -> str:
            formatter = self._get_formatter()

            # description -- in markdown, should come before usage
            formatter.add_text('\n')
            formatter.add_text(self.description)

            # usage
            formatter.add_text('\n')
            formatter.add_usage(self.usage, self._actions, self._mutually_exclusive_groups)

            # positionals, optionals and user-defined groups
            for action_group in self._action_groups:
                formatter.start_section(action_group.title)
                formatter.add_text(action_group.description)
                formatter.add_arguments(action_group._group_actions)
                formatter.end_section()

            # epilog
            formatter.add_text(self.epilog)

            # determine help from format above
            return formatter.format_help()

        # <!-- monkey patch our parser
        # switch format_help, so that stuff comes in an order that makes more sense in markdown
        setattr(parser, 'format_help', MethodType(format_help_markdown, parser))  # noqa: B010
        # switch formatter class so we'll get markdown
        setattr(parser, 'formatter_class', MarkdownFormatter.factory(level))  # noqa: B010
        # -->

        parser.print_help()

        # check if the parser has a subparser, so we can generate its
        # help in markdown as well
        _subparsers = getattr(parser, '_subparsers', None)
        if _subparsers is not None:
            for subparsers in _subparsers._group_actions:
                for subparser in subparsers.choices.values():
                    self.print_help(subparser, level=level + 1)


class MarkdownFormatter(HelpFormatter):
    level: int

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._root_section = self._MarkdownSection(self, None)
        self._current_section = self._root_section

    @staticmethod
    def factory(level: int) -> type[MarkdownFormatter]:
        return type('MarkdownFormatterInstance', (MarkdownFormatter,), {'level': level})

    class _MarkdownSection(HelpFormatter._Section):
        def __init__(self, formatter: MarkdownFormatter, parent: Self | None, heading: str | None = None) -> None:
            self.formatter = formatter
            self.parent = parent
            self.heading = heading
            self.items: list[tuple[Callable[..., str], Iterable[Any]]] = []

        def format_help(self) -> str:
            # format the indented section
            if self.parent is not None:
                self.formatter._indent()
            join = self.formatter._join_parts
            helps: list[str] = []

            # only one table header per section
            print_table_headers = True
            for func, args in self.items:
                item_help_text = func(*args)
                name = getattr(func, '__name__', repr(func))

                # we need to fix headers for argument tables
                if name == '_format_action' and print_table_headers and len(item_help_text) > 0:
                    helps.extend(
                        [
                            '\n',
                            '| argument | default | help |',
                            '\n',
                            '| -------- | ------- | ---- |',
                            '\n',
                        ]
                    )
                    print_table_headers = False

                helps.append(item_help_text)

            item_help = join(helps)

            if self.parent is not None:
                self.formatter._dedent()

            # return nothing if the section was empty
            if not item_help:
                return ''

            # add the heading if the section was non-empty
            if self.heading is not SUPPRESS and self.heading is not None:
                current_indent = self.formatter._current_indent
                heading = '%*s%s\n' % (current_indent, '', self.heading)  # noqa: UP031

                # increase header if we're in a subparser
                assert isinstance(self.formatter, MarkdownFormatter)
                if self.formatter.level > 0:
                    # a bit hackish, to get a line break when adding a subparsers help
                    if self.parent is None:
                        print()

                    heading = f'#{heading}'
            else:
                heading = ''

            # join the section-initial newline, the heading and the help
            return join(['\n', heading, item_help, '\n'])

    @property
    def current_level(self) -> int:
        return self.level + 1

    def _format_usage(self, *args: Any, **kwargs: Any) -> str:
        # remove last argument, which is prefix, we are going to set it
        args = args[:-1]
        args += ('',)
        usage_text = super()._format_usage(*args, **kwargs)

        # wrap usage text in a markdown code block, with bash syntax
        return '\n'.join(
            [
                '',
                f'{"#" * self.current_level}## Usage',
                '',
                '```bash',
                usage_text.strip(),
                '```',
                '',
            ]
        )

    def format_help(self) -> str:
        heading = f'{"#" * self.current_level} `{self._prog}`'
        self._root_section.heading = heading
        return super().format_help()

    def _format_text(self, text: str) -> str:
        if '%(prog)' in text:
            text = text % {'prog': self._prog}

        if len(text.strip()) > 0:
            lines: list[str] = []
            for line in text.split('\n'):
                filled_line = textwrap_fill(line, 120)
                lines.append(filled_line)
            text = '\n'.join(lines)

        return text

    def start_section(self, heading: str | None) -> None:
        if heading is not None:
            heading = f'{heading[0].upper()}{heading[1:]}'  # first letter in first words to upper case
            heading = f'{"#" * self.current_level}# {heading}'

        self._indent()
        section = self._MarkdownSection(self, cast('MarkdownFormatter._MarkdownSection', self._current_section), heading)
        self._add_item(section.format_help, [])
        self._current_section = section

    def _format_action(self, action: Action) -> str:
        # do not include -h/--help or --md-help in the markdown
        # help
        if 'help' in action.dest or action.dest == SUPPRESS:
            return ''

        lines: list[str] = []

        if action.help is not None:
            expanded_help = self._expand_help(action)
            help_text = self._split_lines(expanded_help, 80)
        else:
            help_text = ['']

        argument = ', '.join(action.option_strings) if getattr(action, 'option_strings', None) is not None and len(action.option_strings) > 0 else action.dest
        default = f'`{action.default}`' if action.default is not None else ''
        help_value = '<br/>'.join(help_text)

        # format arguments as a markdown table row
        lines.extend([f'| `{argument}` | {default} | {help_value} |', ''])

        return '\n'.join(lines)
