"""Functionality for bash completion logic in grizzly argparse wrapper."""

from __future__ import annotations

import sys
from argparse import (
    SUPPRESS,
    Action,
    ArgumentError,
    ArgumentParser,
    Namespace,
    _AppendAction,
    _StoreAction,
    _StoreConstAction,
    _SubParsersAction,
)
from collections.abc import Sequence
from os import path
from pathlib import Path
from typing import Any, Union, cast

from grizzly_cli.argparse.bashcompletion.types import BashCompletionTypes

__all__ = [
    'BashCompleteAction',
    'BashCompletionAction',
    'BashCompletionTypes',
    'hook',
]


class BashCompletionAction(Action):
    def __init__(
        self,
        option_strings: list[str],
        dest: str = SUPPRESS,
        default: str = SUPPRESS,
        help_text: str = SUPPRESS,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            option_strings=option_strings,
            dest=dest,
            default=default,
            help=help_text,
            nargs=0,
            **kwargs,
        )

    def __call__(
        self,
        parser: ArgumentParser,
        *_args: Any,
        **_kwargs: Any,
    ) -> None:
        current_file = Path(__file__)
        file_directory = current_file.parent
        bash_script = file_directory / 'bashcompletion.bash'
        with bash_script.open(encoding='utf-8') as fd:
            print(fd.read().replace('bashcompletion_template', parser.prog))

        parser.exit()


class BashCompleteAction(Action):
    def __init__(
        self,
        option_strings: list[str],
        dest: str = SUPPRESS,
        default: str = SUPPRESS,
        help_text: str = SUPPRESS,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            option_strings=option_strings,
            dest=dest,
            default=default,
            help=help_text,
            nargs=None,
            **kwargs,
        )

    def get_suggestions(self, parser: ArgumentParser) -> dict[str, Union[str, Action]]:
        suggestions: dict[str, Union[str, Action]] = {}

        for action in parser._actions:
            if isinstance(action, (BashCompleteAction, BashCompletionAction)) or (SUPPRESS in [action.help, action.default] and action.dest != 'help'):
                continue

            if isinstance(action, _SubParsersAction):
                suggestions.update(dict.fromkeys(action.choices.keys(), action))
            elif len(action.option_strings) > 0:
                suggestions.update(dict.fromkeys(action.option_strings, action))
            else:
                suggestions.update({action.dest: action})

        return suggestions

    def get_exclusive_suggestions(self, parser: ArgumentParser) -> dict[str, list[str]]:
        exclusive_suggestions: dict[str, list[str]] = {}
        for group in parser._mutually_exclusive_groups:
            exclusives: list[str] = []
            for action in group._group_actions:
                exclusives.extend(action.option_strings)

            for exclusive in exclusives:
                exclusive_suggestions.update({exclusive: list(filter(lambda x: x != exclusive, exclusives))})

        return exclusive_suggestions

    def get_provided_options(self, prog: str, values: Union[str, Sequence[Any], None]) -> list[str]:
        options: list[str] = []

        if isinstance(values, str):
            options = [value for value in values.replace(f'{prog}', '').split(' ') if len(value.strip()) > 0]
        elif isinstance(values, Sequence):
            options = [str(value) for value in values if len(str(value).strip()) > 0 and prog not in value]

        return options

    def remove_completed(self, provided_options: list[str], suggestions: dict[str, Union[str, Action]], exclusive_suggestions: dict[str, list[str]]) -> list[str]:  # noqa: C901, PLR0912
        if len(provided_options) <= 1:
            return provided_options

        filtered_options: list[str] = []
        skip: bool = False
        concat: bool = False

        for index, option in enumerate(provided_options):
            add_next = False
            remove_suggestion = False

            if concat:
                concat = False
                option = f'{provided_options[index - 1]} {option}'  # noqa: PLW2901

            if len(option) < 1 or skip:
                skip = False
                continue

            suggestion = suggestions.get(option)

            if suggestion is not None:
                if isinstance(suggestion, _AppendAction):
                    # skip next supplied value, since it is part of the option
                    # also, _AppendAction can be specified more than once
                    if len(provided_options) > index + 1 and not provided_options[index + 1].strip().startswith('-'):
                        skip = True
                        continue
                elif isinstance(suggestion, _StoreAction):
                    if index == len(provided_options) - 1:
                        pass
                    elif index == len(provided_options) - 2:  # beginning of value should also be added
                        add_next = True
                    else:  # completed, do not add
                        remove_suggestion = True
                else:
                    remove_suggestion = True

                if remove_suggestion and isinstance(suggestion, Action):
                    # remove all other, completed, options from suggestion
                    for suggestion_option in suggestion.option_strings:
                        suggestions.pop(suggestion_option, None)

                        # remove options that are mutually exclusive to completed option
                        exclusive_removes = exclusive_suggestions.get(suggestion_option, [])
                        for exclusive_option in exclusive_removes:
                            suggestions.pop(exclusive_option, None)
                    continue
            elif not any(suggested_option.startswith(option) for suggested_option in suggestions):  # could be values for an option
                remove = True
                if option.endswith('\\') and sys.platform != 'win32':
                    concat = True
                    continue

                for suggestion in suggestions.values():
                    if isinstance(suggestion, Action) and len(suggestion.option_strings) == 0 and isinstance(suggestion.type, BashCompletionTypes.File):
                        file_suggestions = suggestion.type.list_files(option)
                        for file in file_suggestions:
                            if file.startswith(option):
                                remove = False
                                break

                if remove:
                    continue

            filtered_options.append(option)
            if add_next:
                filtered_options.append(provided_options[index + 1])
                skip = True

        return filtered_options

    def filter_suggestions(self, provided_options: list[str], suggestions: dict[str, Union[str, Action]]) -> dict[str, Union[str, Action]]:
        if len(provided_options) < 1:
            return suggestions

        filtered_suggestions: dict[str, Union[str, Action]] = {}
        for option in provided_options:
            for option_suggestion, suggestion in suggestions.items():
                if option_suggestion.startswith(option) or (
                    not option.startswith('-') and isinstance(suggestion, Action) and len(suggestion.option_strings) == 0 and not isinstance(suggestion, _SubParsersAction)
                ):
                    filtered_suggestions.update({option_suggestion: suggestion})

        return filtered_suggestions

    def __call__(  # noqa: C901, PLR0912, PLR0915
        self,
        parser: ArgumentParser,
        namespace: Namespace,  # noqa: ARG002
        values: Union[str, Sequence[Any], None],
        option_string: str | None = None,  # noqa: ARG002
    ) -> None:
        all_suggestions: dict[str, Union[str, Action]] = {}
        provided_options = self.get_provided_options(parser.prog, values)
        suggestions = self.get_suggestions(parser)

        if '-h' in provided_options or '--help' in provided_options:
            print()
            parser.exit()

        # map options that are only allowed by it own
        exclusive_suggestions = self.get_exclusive_suggestions(parser)

        # check for positional arguments (e.g. no option string)

        # remove any completed values, or exclusive suggestions
        provided_options = self.remove_completed(provided_options, suggestions, exclusive_suggestions)

        # all, remaining, suggestions -- completed has been removed
        all_suggestions = suggestions.copy()

        # only return suggestions that matches supplied value
        suggestions = self.filter_suggestions(provided_options, suggestions)

        if len(provided_options) > 0:
            suggestion = suggestions.get(provided_options[0], None)

            if suggestion is not None:
                # based option type
                if isinstance(suggestion, _StoreConstAction):
                    suggestions = all_suggestions
                    for option in suggestion.option_strings:
                        del suggestions[option]
                elif isinstance(suggestion, (_AppendAction, _StoreAction)):
                    # value for append action has been provided
                    suggestions = all_suggestions if len(provided_options) == 2 else {}

            # based on option value type
            suggestion = all_suggestions.get(provided_options[0], None)
            if isinstance(suggestion, Action) and suggestion.type is not None:
                value = provided_options[-1] if len(provided_options) > 1 else None
                if isinstance(suggestion.type, BashCompletionTypes.File):
                    file_suggestions = suggestion.type.list_files(value)

                    if not (len(file_suggestions) == 1 and provided_options[-1] in file_suggestions):
                        suggestions = cast('dict[str, Union[str, Action]]', file_suggestions)
                    else:
                        suggestions = all_suggestions
                        for option in suggestion.option_strings:
                            del suggestions[option]
                elif (suggestion.type is str and not isinstance(value, str)) or (suggestion.type is int and (value is None or not value.isnumeric())):
                    suggestions = {}

                if value is not None and isinstance(suggestion, _StoreAction):
                    for option in suggestion.option_strings:
                        if option in suggestions:
                            del suggestions[option]

        # check for positionals
        original_suggestions = suggestions.copy()
        for option, suggestion in original_suggestions.items():
            if (option.startswith('-') and isinstance(suggestion, Action)) or (not option.startswith('-') and isinstance(suggestion, (_SubParsersAction, str))):
                continue

            del suggestions[option]

            if isinstance(suggestion, Action) and suggestion.type is not None and isinstance(suggestion.type, BashCompletionTypes.File):
                value = provided_options[-1] if len(provided_options) == 1 and not provided_options[-1].startswith('-') else None

                if (value is None and len(provided_options) == 0) or (value is not None and len(provided_options) == 1):
                    file_suggestions = suggestion.type.list_files(value)
                    value_type = file_suggestions.get(value, None) if value is not None else None

                    # check if suggestion matching provided option (value) is a directory, and if
                    # provded option (value) does not end with a path separator, it should be added
                    # otherwise it will not be completed correctly
                    if value_type == 'dir' and (value is not None and not value.endswith(path.sep)):
                        value = f'{value}{path.sep}'

                    # only provide further suggestions if matches isn't a completed file path
                    if not (len(file_suggestions) == 1 and value in file_suggestions) and (value_type is None or value_type != 'file'):
                        suggestions.update(file_suggestions)
                    else:
                        suggestions = all_suggestions
                        del suggestions[suggestion.dest]

        print('\n'.join(suggestions.keys()))
        parser.exit()


def hook(parser: ArgumentParser) -> None:
    try:
        parser.add_argument('--bash-complete', action=BashCompleteAction)
    except ArgumentError as e:
        # we've already "hooked" the parser
        if 'conflicting option string: --bash-complete' not in e.message:
            raise
    except Exception:
        raise
    finally:
        _subparsers = getattr(parser, '_subparsers', None)
        if _subparsers is not None:
            for subparsers in _subparsers._group_actions:
                for subparser in subparsers.choices.values():
                    hook(subparser)
