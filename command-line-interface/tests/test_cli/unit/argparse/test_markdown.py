"""Tests for grizzly_cli.argparse.markdown."""

from __future__ import annotations

import argparse
from typing import TYPE_CHECKING

import pytest
from grizzly_cli.argparse.markdown import MarkdownFormatter, MarkdownHelpAction

if TYPE_CHECKING:
    from _pytest.capture import CaptureFixture
    from pytest_mock import MockerFixture


class TestMarkdownHelpAction:
    def test___init__(self) -> None:
        action = MarkdownHelpAction(['-t', '--test'])

        assert isinstance(action, argparse.Action)
        assert action.option_strings == ['-t', '--test']
        assert action.dest == argparse.SUPPRESS
        assert action.default == argparse.SUPPRESS
        assert action.nargs == 0

    def test___call__(self, mocker: MockerFixture) -> None:
        parser = argparse.ArgumentParser(description='test parser')
        parser.add_argument('--md-help', action=MarkdownHelpAction)

        print_help = mocker.patch.object(parser._actions[-1], 'print_help', autospec=True)

        with pytest.raises(SystemExit) as e:
            parser.parse_args(['--md-help'])
        assert e.type is SystemExit
        assert e.value.code == 0

        assert print_help.call_count == 1
        args, _ = print_help.call_args_list[0]
        assert args[0] is parser

    def test_print_help(self, mocker: MockerFixture) -> None:
        action = MarkdownHelpAction(['-t', '--test'])
        parser = argparse.ArgumentParser(description='test parser')
        parser.add_argument('--md-help', action=MarkdownHelpAction)

        subparsers = parser.add_subparsers(dest='subparser')

        a_parser = subparsers.add_parser('a', description='parser a')
        subparsers.add_parser('b', description='parser b')

        a_subparsers = a_parser.add_subparsers(dest='a_subparsers')
        a_subparsers.add_parser('aa', description='parser aa')

        print_help = mocker.patch('argparse.ArgumentParser.print_help', autospec=True)

        action.print_help(parser)

        assert print_help.call_count == 4
        assert issubclass(parser.formatter_class, MarkdownFormatter)  # type: ignore[arg-type]
        assert parser._subparsers is not None

        _subparsers = getattr(parser, '_subparsers', None)
        assert _subparsers is not None
        for subparsers in _subparsers._group_actions:
            for name, subparser in subparsers.choices.items():
                assert issubclass(subparser.formatter_class, MarkdownFormatter)
                if name == 'a':
                    _subsubparsers = getattr(subparser, '_subparsers', None)
                    assert _subsubparsers is not None
                    for subsubparsers in _subsubparsers._group_actions:
                        for subsubparser in subsubparsers.choices.values():
                            assert issubclass(subsubparser.formatter_class, MarkdownFormatter)

    def test_print_help__format_help_markdown(self, mocker: MockerFixture) -> None:
        action = MarkdownHelpAction(['-t', '--test'])
        parser = argparse.ArgumentParser(description='test parser')
        parser._optionals.title = 'optional arguments'

        formatter = MarkdownFormatter.factory(0)('test-prog')

        _get_formatter = mocker.patch.object(parser, '_get_formatter', side_effect=[formatter])
        add_text = mocker.patch.object(formatter, 'add_text', autospec=True)
        add_usage = mocker.patch.object(formatter, 'add_usage', autospec=True)
        start_section = mocker.patch.object(formatter, 'start_section', autospec=True)
        end_section = mocker.patch.object(formatter, 'end_section', autospec=True)
        add_arguments = mocker.patch.object(formatter, 'add_arguments', autospec=True)

        action.print_help(parser)

        assert _get_formatter.call_count == 1
        assert add_text.call_count == 6
        assert add_usage.call_count == 1
        assert start_section.call_count == 2
        assert start_section.call_args_list[0][0][0] == 'positional arguments'
        assert start_section.call_args_list[1][0][0] == 'optional arguments'
        assert end_section.call_count == 2
        assert add_arguments.call_count == 2


class TestMarkdownFormatter:
    def test___init__(self) -> None:
        formatter = MarkdownFormatter.factory(0)('test')
        assert formatter._root_section is formatter._current_section
        assert formatter._root_section.parent is None
        assert formatter.level == 0
        assert formatter.current_level == 1

    def test__format_usage(self) -> None:
        formatter = MarkdownFormatter.factory(0)('test')
        usage = formatter._format_usage('test', None, None, 'a prefix')
        assert (
            usage
            == """
### Usage

```bash
test
```
"""
        )
        parser = argparse.ArgumentParser(prog='test', description='test parser')
        parser.add_argument('-t', '--test', type=str, required=True, help='test argument')
        parser.add_argument('file', nargs=1, help='file argument')

        core_formatter = parser.formatter_class(prog=parser.prog)

        usage = core_formatter._format_usage(parser.usage, parser._get_positional_actions(), parser._mutually_exclusive_groups, 'a prefix ')
        assert (
            usage
            == """a prefix test file

"""
        )

        usage = formatter._format_usage(parser.usage, parser._get_positional_actions(), parser._mutually_exclusive_groups, 'a prefix ')
        assert (
            usage
            == """
### Usage

```bash
test file
```
"""
        )

    def test_format_help(self) -> None:
        formatter = MarkdownFormatter.factory(0)('test')
        assert formatter.format_help() == ''
        assert formatter._root_section.heading == '# `test`'

    def test_format_text(self) -> None:
        formatter = MarkdownFormatter('test-prog')
        text = """%(prog)s is awesome!
also, here is a sentence. and here is another one!

```bash
hostname -f
```

you cannot belive it, it's another sentence.
"""
        print(formatter._format_text(text))
        assert (
            formatter._format_text(text)
            == """test-prog is awesome!
also, here is a sentence. and here is another one!

```bash
hostname -f
```

you cannot belive it, it's another sentence.
"""
        )

    def test_start_section(self) -> None:
        formatter = MarkdownFormatter.factory(0)('test-prog')
        assert formatter._root_section is formatter._current_section

        formatter.start_section('test-section-01')

        assert formatter._current_section is not formatter._root_section
        assert formatter._current_section.parent is formatter._root_section
        assert formatter._current_section.heading == '## Test-section-01'
        assert len(formatter._current_section.items) == 0
        assert next(iter(formatter._current_section.parent.items)) == (formatter._current_section.format_help, [])

    def test__format_action(self) -> None:
        formatter = MarkdownFormatter.factory(0)('test-prog')
        action = argparse.Action(['-t', '--test'], dest='help', nargs=1, help='test argument')

        assert formatter._format_action(action) == ''

        action.dest = 'test'

        assert formatter._format_action(action) == '| `-t, --test` |  | test argument |\n'

        action.default = 'test-default'
        action.option_strings = ['-t']

        assert formatter._format_action(action) == '| `-t` | `test-default` | test argument |\n'

    def test_current_level(self) -> None:
        formatter = MarkdownFormatter('test-prog')
        formatter.level = 10
        assert formatter.current_level == 11

    class Test_MarkdownSection:
        def test___init__(self) -> None:
            formatter = MarkdownFormatter('test-prog')
            section1 = MarkdownFormatter._MarkdownSection(formatter, None)

            assert section1.formatter is formatter
            assert section1.parent is None
            assert section1.heading is None
            assert len(section1.items) == 0

            section2 = MarkdownFormatter._MarkdownSection(formatter, section1)
            assert section2.formatter is formatter
            assert section2.parent is section1
            assert section2.heading is None
            assert len(section2.items) == 0

            section3 = MarkdownFormatter._MarkdownSection(formatter, section2, 'test heading')
            assert section3.formatter is formatter
            assert section3.parent is section2
            assert section3.heading == 'test heading'
            assert len(section3.items) == 0

        def test_format_help(self, capsys: CaptureFixture) -> None:
            formatter = MarkdownFormatter.factory(0)('test-prog')

            action1 = argparse.Action(['-r', '--root'], dest='root', nargs=2, help='root argument')
            action2 = argparse.Action(['--root-const'], dest='root', nargs=0, default=True)

            formatter.start_section('root section')
            formatter._add_item(formatter._format_action, [action1])
            formatter._add_item(formatter._format_action, [action2])
            formatter.end_section()

            format_help_text = formatter._current_section.format_help()
            assert capsys.readouterr().out == ''
            assert (
                format_help_text
                == """

## Root section

| argument | default | help |
| -------- | ------- | ---- |
| `-r, --root` |  | root argument |
| `--root-const` | `True` |  |


"""
            )
            formatter = MarkdownFormatter('test-prog')
            formatter.level = 1

            action1 = argparse.Action(['-r', '--root'], dest='root', nargs=2, help='root argument')
            action2 = argparse.Action(['--root-const'], dest='root', nargs=0, default=True)

            formatter.start_section('root section')
            formatter._add_item(formatter._format_action, [action1])
            formatter._add_item(formatter._format_action, [action2])
            parent = formatter._current_section.parent
            formatter._current_section.parent = None
            formatter.end_section()
            assert parent is not None
            formatter._current_section = parent
            format_help_text = formatter._current_section.format_help()
            assert capsys.readouterr().out == '\n'  # @TODO: whyyyyyyyyyy?!
            print(format_help_text)
            assert (
                format_help_text
                == """

#### Root section

| argument | default | help |
| -------- | ------- | ---- |
| `-r, --root` |  | root argument |
| `--root-const` | `True` |  |


"""
            )
