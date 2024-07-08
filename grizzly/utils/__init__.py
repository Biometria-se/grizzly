"""Module contains utils."""
from __future__ import annotations

import logging
import re
from collections.abc import Generator, Iterable, Mapping
from contextlib import contextmanager, suppress
from copy import deepcopy
from importlib import import_module
from os import environ
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, Generic, Optional, Union, cast
from unicodedata import normalize as __normalize

from jinja2.lexer import Token, TokenStream
from jinja2_simple_tags import StandaloneTag
from locust.stats import STATS_NAME_WIDTH

from grizzly.types import T

if TYPE_CHECKING:  # pragma: no cover
    from datetime import datetime

    from grizzly.context import GrizzlyContextScenario
    from grizzly.scenarios import GrizzlyScenario
    from grizzly.types.behave import Context, StepFunctionType
    from grizzly.users import GrizzlyUser


logger = logging.getLogger(__name__)


class ModuleLoader(Generic[T]):
    @staticmethod
    def load(default_module: str, value: str) -> type[T]:
        """Dynamically load a module based on namespace (value)."""
        try:
            [module_name, class_name] = value.rsplit('.', 1)
        except ValueError:
            module_name = default_module
            class_name = value

        if class_name not in globals():
            module = import_module(module_name)
            globals()[class_name] = getattr(module, class_name)

        class_type_instance = globals()[class_name]

        return cast(type[T], class_type_instance)

class MergeYamlTag(StandaloneTag):
    tags: ClassVar[set[str]] = {'merge'}

    def preprocess(
        self, source: str, name: Optional[str], filename: Optional[str] = None,
    ) -> str:
        self._source = source
        return cast(str, super().preprocess(source, name, filename))

    def render(self, filename: str, *filenames: str) -> str:
        buffer: list[str] = []

        files = [filename, *filenames]

        for file in files:
            merge_file = Path(file)

            # check if relative to parent feature file
            if not merge_file.exists():
                merge_file = (self.environment.source_file.parent / merge_file).resolve()

            if not merge_file.exists():
                raise FileNotFoundError(merge_file)

            merge_content = merge_file.read_text()

            if merge_content[0:3] != '---':
                buffer.append('---')

            buffer.append(merge_content)

        if self._source[0:3] != '---':
            buffer.append('---')

        return '\n'.join(buffer)

    def filter_stream(self, stream: TokenStream) -> Union[TokenStream, Iterable[Token]]:
        """Everything outside of `{% merge ... %}` should be treated as "data", e.g. plain text."""
        in_merge = False
        in_variable = False
        in_block_comment = False

        variable_begin_pos = -1
        variable_end_pos = 0
        block_begin_pos = -1
        block_end_pos = 0
        source_lines = self._source.splitlines()

        for token in stream:
            if token.type == 'block_begin' and stream.current.value in self.tags:
                in_merge = True
                current_line = source_lines[token.lineno - 1].lstrip()
                in_block_comment = current_line.startswith('#')
                block_begin_pos = self._source.index(token.value, block_begin_pos + 1)

            if not in_merge:
                if token.type == 'variable_end':
                    # Find variable end in the source
                    variable_end_pos = self._source.index(token.value, variable_begin_pos)
                    # Extract the variable definition substring and use as token value
                    token_value = self._source[variable_begin_pos:variable_end_pos + len(token.value)]
                    in_variable = False
                elif token.type == 'variable_begin':
                    # Find variable start in the source
                    variable_begin_pos = self._source.index(token.value, variable_begin_pos + 1)
                    in_variable = True
                else:
                    token_value = token.value

                if in_variable:
                    # While handling in-variable tokens, withhold values until
                    # the end of the variable is reached
                    continue

                filtered_token = Token(token.lineno, 'data', token_value)
            elif token.type == 'block_end' and in_block_comment:
                in_block_comment = False
                block_end_pos = self._source.index(token.value, block_begin_pos)
                token_value = self._source[block_begin_pos:block_end_pos + len(token.value)]
                filtered_token = Token(token.lineno, 'data', token_value)
            elif in_block_comment:
                continue
            else:
                filtered_token = token

            yield filtered_token

            if in_merge and token.type == 'block_end':
                in_merge = False


@contextmanager
def fail_direct(context: Context) -> Generator[None, None, None]:
    """Context manager used to stop behave directly on failure in critical code paths."""
    # save original values
    orig_stop_value = context.config.stop
    orig_verbose_value = context.config.verbose

    # do not continue with other features, stop
    context.config.stop = True
    # we do not want stacktrace for this hook-error, if the encapsuled assert fails
    context.config.verbose = False

    try:
        yield None
    finally:
        pass

    # only restore if the ecapsuled assert passes
    context.config.stop = orig_stop_value
    context.config.verbose = orig_verbose_value


def create_user_class_type(scenario: GrizzlyContextScenario, global_context: Optional[dict[str, Any]] = None, fixed_count: Optional[int] = None) -> type[GrizzlyUser]:
    """Create a unique (name wise) class, that locust will use to create user instances."""
    if not hasattr(scenario, 'user') or scenario.user is None:
        message = f'scenario {scenario.description} has not set a user'
        raise ValueError(message)

    if not hasattr(scenario.user, 'class_name') or scenario.user.class_name is None:
        message = f'scenario {scenario.description} does not have a user type set'
        raise ValueError(message)

    base_user_class_type = cast(type['GrizzlyUser'], ModuleLoader['GrizzlyUser'].load('grizzly.users', scenario.user.class_name))  # type: ignore[redundant-cast]
    user_class_name = f'{scenario.user.class_name}_{scenario.identifier}'

    context: dict[str, Any] = {}
    contexts: list[dict[str, Any]] = [
        base_user_class_type.__context__,
        global_context or {},
        scenario.context,
    ]

    if fixed_count is None:
        fixed_count = scenario.user.fixed_count

    for merge_context in contexts:
        logger.debug('%s context: %r', user_class_name, merge_context)
        context = merge_dicts(context, merge_context)

    distribution: dict[str, Union[int, float, str | None]] = {
        'weight': scenario.user.weight,
        'sticky_tag': scenario.user.sticky_tag,
    }

    if fixed_count is not None:
        distribution.update({'fixed_count': fixed_count})

    return type(user_class_name, (base_user_class_type, ), {
        '__module__': base_user_class_type.__module__,
        '__dependencies__': base_user_class_type.__dependencies__,
        '__scenario__': scenario,
        '__context__': context,
        **distribution,
    })


def create_scenario_class_type(base_type: str, scenario: GrizzlyContextScenario) -> type[GrizzlyScenario]:
    """Create a unique (name wise) class, that the created user type will use as a scenario."""
    if base_type.count('.') > 0:
        module, base_type = base_type.rsplit('.', 1)
    else:
        module = 'grizzly.scenarios'

    base_task_class_type = ModuleLoader['GrizzlyScenario'].load(module, base_type)
    task_class_name = f'{base_type}_{scenario.identifier}'

    return type(task_class_name, (base_task_class_type, ), {
        '__module__': base_task_class_type.__module__,
        'pace_time': scenario.pace,
        'tasks': [],
    })


def merge_dicts(merged: dict[str, Any], source: dict[str, Any]) -> dict[str, Any]:
    """Merge two dicts recursively, where `source` values takes precedance over `merged` values."""
    merged = deepcopy(merged)
    source = deepcopy(source)

    for key in source:
        if (
            key in merged
            and isinstance(merged[key], dict)
            and isinstance(source[key], Mapping)
        ):
            merged[key] = merge_dicts(merged[key], source[key])
        else:
            value = source[key]
            if isinstance(value, str) and value.lower() == 'none':
                value = None
            merged[key] = value

    return merged


def in_correct_section(func: StepFunctionType, expected: list[str]) -> bool:
    """Check if a step function is used in the correct section of the feature file, as specified in `expected` (list of namespaces)."""
    try:
        actual = '.'.join(func.__module__.rsplit('.', 1)[:-1])
    except AttributeError:  # function does not belong to a module
        actual = 'custom'

    return (
        actual.startswith('grizzly.') and actual in expected
    ) or not actual.startswith('grizzly.')


def parse_timespan(timespan: str) -> dict[str, int]:
    """Parse a timespan string (e.g. 1Y2M) to a dictionary representing each time part."""
    if re.match(r'^-?\d+$', timespan):
        # if an int is specified we assume they want days
        return {'days': int(timespan)}

    pattern = re.compile(r'((?P<years>-?\d+?)Y)?((?P<months>-?\d+?)M)?((?P<days>-?\d+?)D)?((?P<hours>-?\d+?)h)?((?P<minutes>-?\d+?)m)?((?P<seconds>-?\d+?)s)?')
    parts = pattern.match(timespan)
    if not parts:
        message = f'invalid time span format: {timespan}'
        raise ValueError(message)
    group = parts.groupdict()
    parameters = {name: int(value) for name, value in group.items() if value}
    if not parameters:
        message = f'invalid time span format: {timespan}'
        raise ValueError(message)

    return parameters


def _print_table(subject: str, header: str, data: list[tuple[datetime, str]]) -> None:
    if len(data) < 1:
        return

    from grizzly.locust import stats_logger

    data = sorted(data, key=lambda k: k[0])

    stats_logger.info(f'{subject}:')
    stats_logger.info('%-20s %-100s' % ('Timestamp (UTC)', header))
    separator = f'{"-" * 20}|{"-" * ((80 + STATS_NAME_WIDTH) - 19)}'
    stats_logger.info(separator)

    for timestamp, info in data:
        stats_logger.info('%-20s %-100s' % (timestamp.strftime('%Y-%m-%d %H:%M:%S'), info))
    stats_logger.info(separator)
    stats_logger.info('')

    for handler in stats_logger.handlers:
        handler.flush()


def safe_del(struct: dict[str, Any], key: str) -> None:
    """Remove a key from a dictionary, but do not fail if it does not exist."""
    with suppress(KeyError):
        del struct[key]


def has_template(text: str) -> bool:
    """Check if given text contains any jinja2 templates."""
    return '{{' in text and '}}' in text


def has_parameter(text: str) -> bool:
    sep_count = text.count('::')
    boundary_count = text.count('$')
    return sep_count > 0 and boundary_count / 2 == sep_count


def is_file(text: str) -> bool:
    base_dir = environ.get('GRIZZLY_CONTEXT_ROOT', None)

    if base_dir is None or len(text.strip()) < 1:
        return False

    try:
        file = Path(base_dir) / 'requests' / text
        return file.exists()
    except (OSError, FileNotFoundError):
        return False


def flatten(node: dict[str, Any], parents: Optional[list[str]] = None) -> dict[str, Any]:
    """Flatten a dictionary so each value key is the path down the nested dictionary structure."""
    flat: dict[str, Any] = {}
    if parents is None:
        parents = []

    for key, value in node.items():
        parents.append(key)
        if isinstance(value, dict):
            flat = {**flat, **flatten(value, parents)}
        else:
            flat['.'.join(parents)] = value

        parents.pop()

    return flat

def unflatten(key: str, value: Any) -> dict[str, Any]:
    paths: list[str] = key.split('.')

    # last node should have the value
    path = paths.pop()
    struct = {path: value}

    # build the struct from the inside out
    paths.reverse()

    for path in paths:
        struct = {path: {**struct}}

    return struct


def normalize(value: str) -> str:
    """Normalize a string to make it more non-human friendly."""
    value = __normalize('NFKD', str(value)).encode('ascii', 'ignore').decode('ascii')
    value = re.sub(r'[^\w\s-]', '', value)

    return re.sub(r'[-\s]+', '-', value).strip('-_')
