from __future__ import annotations

import inspect
import re
import warnings
from importlib import import_module
from os import sep
from pathlib import Path, PurePath
from typing import TYPE_CHECKING, Any, cast

from behave.i18n import languages
from behave.matchers import ParseMatcher
from behave.runner_util import load_step_modules as behave_load_step_modules

from grizzly_ls.model import Step
from grizzly_ls.text import (
    Coordinate,
    NormalizeHolder,
    Normalizer,
    RegexPermutationResolver,
    clean_help,
)

if TYPE_CHECKING:
    from collections.abc import Iterable
    from types import ModuleType

    from grizzly_ls.server import GrizzlyLanguageServer


def load_step_registry(step_paths: list[Path]) -> dict[str, list[ParseMatcher]]:
    from behave import step_registry  # noqa: PLC0415

    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        behave_load_step_modules([str(step_path) for step_path in step_paths])

    return cast('dict[str, list[ParseMatcher]]', step_registry.registry.steps.copy())


def create_step_normalizer(ls: GrizzlyLanguageServer) -> Normalizer:
    custom_type_permutations: dict[str, NormalizeHolder] = {}

    for custom_type, func in ParseMatcher.TYPE_REGISTRY.items():
        func_code = [line for line in inspect.getsource(func).strip().split('\n') if not line.strip().startswith('@classmethod')]

        if func_code[0].startswith('@parse.with_pattern'):
            match = re.match(r'@parse.with_pattern\(r\'\(?(.*?)\)?\'', func_code[0])
            if match:
                pattern = match.group(1)
                vector = getattr(func, '__vector__', None)
                if vector is None:
                    coordinates = Coordinate()
                else:
                    x, y = vector
                    coordinates = Coordinate(x=x, y=y)

                custom_type_permutations.update(
                    {
                        custom_type: NormalizeHolder(
                            permutations=coordinates,
                            replacements=RegexPermutationResolver.resolve(pattern),
                        ),
                    }
                )
            else:
                message = f'could not extract pattern from "{func_code[0]}" for custom type {custom_type}'
                raise ValueError(message)
        elif 'from_string(' in func_code[-1] or 'from_string(' in func_code[0]:
            enum_name: str

            match = re.match(r'return ([^\.]*)\.from_string\(', func_code[-1].strip())
            module: ModuleType | None
            if match:
                enum_name = match.group(1)
                module = import_module('grizzly.types')
            else:
                match = re.match(
                    r'def from_string.*?->\s+\'?([^:\']*)\'?:',
                    func_code[0].strip(),
                )
                if match:
                    enum_name = match.group(1)
                    module = inspect.getmodule(func)
                else:
                    message = f'could not find the type that from_string method for custom type {custom_type} returns'
                    raise ValueError(message)

            enum_class = getattr(module, enum_name)

            def enum_value_getter(v: Any) -> str:
                try:
                    if not callable(getattr(v, 'get_value', None)):
                        raise NotImplementedError
                    enum_value = v.get_value()
                except NotImplementedError:
                    enum_value = v.name.lower()

                return cast('str', enum_value)

            replacements = [enum_value_getter(value) for value in enum_class]
            vector = enum_class.get_vector()

            if vector is None:
                coordinates = Coordinate()
            else:
                x, y = vector
                coordinates = Coordinate(x=x, y=y)

            custom_type_permutations.update(
                {
                    custom_type: NormalizeHolder(
                        permutations=coordinates,
                        replacements=replacements,
                    ),
                }
            )

    return Normalizer(ls, custom_type_permutations)


def _match_path(path: Path, pattern: str) -> bool:
    return any(PurePath(sep.join(path.parts[: i + 2])).match(pattern) for i in range(len(path.parts) - 1))  # noqa: PTH118


def _filter_source_directories(file_ignore_patterns: list[str], source_file_paths: Iterable[Path]) -> set[Path]:
    # Ignore [unix] hidden files, node_modules and bin by default
    if not file_ignore_patterns:
        file_ignore_patterns = [
            '**/.*',
            '**/node_modules',
            '**/bin',
        ]

    return {path.parent for path in source_file_paths if path.parent.is_dir() and all(not _match_path(path.parent, ignore_pattern) for ignore_pattern in file_ignore_patterns)}


def compile_inventory(ls: GrizzlyLanguageServer, *, standalone: bool = False) -> None:
    ls.logger.debug('creating step registry')
    project_name = ls.root_path.stem

    try:
        ls.behave_steps.clear()
        paths = _filter_source_directories(ls.file_ignore_patterns, ls.root_path.rglob('*.py'))

        plain_paths = [path.as_posix() for path in paths]

        ls.logger.debug(f'loading steps from {plain_paths}')

        try:
            from gevent import monkey  # noqa: PLC0415

            ls.logger.info('found gevent, applying monkey patching')

            monkey.patch_all()
        except ModuleNotFoundError:
            pass
        except Exception:
            ls.logger.exception('failed to apply gevent monkey patching')

        # ignore paths that contains errors
        for path in paths:
            try:
                ls.behave_steps.update(load_step_registry([path]))
            except Exception as e:  # noqa: PERF203
                ls.logger.exception(f'failed to load steps from {path}:\n{e!s}')
    except Exception as e:
        if not standalone:
            ls.logger.exception(
                f'unable to load behave step expressions:\n{e!s}',
                notify=True,
            )
            return

        raise

    try:
        ls.normalizer = create_step_normalizer(ls)
    except ValueError:
        if not standalone:
            ls.logger.exception('unable to normalize step expression', notify=True)
            return

        raise

    compile_step_inventory(ls)

    total_steps = 0
    for steps in ls.steps.values():
        total_steps += len(steps)

    compile_keyword_inventory(ls)

    ls.logger.info(f'found {len(ls.keywords)} keywords and {total_steps} steps in "{project_name}"')


def compile_step_inventory(ls: GrizzlyLanguageServer) -> None:
    for keyword, steps in ls.behave_steps.items():
        normalized_steps_all: list[Step] = []
        for step in steps:
            normalized_steps = ls._normalize_step_expression(step)
            steps_holder: list[Step] = []

            for normalized_step in normalized_steps:
                help_text = getattr(step.func, '__doc__', None)

                if help_text is not None:
                    help_text = clean_help(help_text)

                step_holder = Step(
                    keyword,
                    normalized_step,
                    func=step.func,
                    help=help_text,
                )
                steps_holder.append(step_holder)

            normalized_steps_all += steps_holder

        ls.steps.update({keyword: normalized_steps_all})


def compile_keyword_inventory(ls: GrizzlyLanguageServer) -> None:
    ls.localizations = languages.get(ls.language, {})
    if ls.localizations == {}:
        message = f'unknown language "{ls.language}"'
        raise ValueError(message)

    # make sure that all keywords doesn't have any whitespace
    for key, values in {**ls.localizations}.items():
        ls.localizations.update({key: [v.strip() for v in values] if isinstance(values, list) else values.strip()})

    # localized any keywords
    ls.__class__.keywords_any = list(
        {
            '*',
            *ls.localizations.get('but', []),
            *ls.localizations.get('and', []),
        }
    )

    ls.__class__.keywords_headers = []
    ls.__class__.keywords_all = []
    for key, values in ls.localizations.items():
        if values[0] != '*':
            ls.keywords_headers.extend([*ls.localizations.get(key, [])])
            ls.keywords_all.extend([*values])
        else:
            ls.keywords_all.extend([*values[1:]])

    # localized keywords
    ls.keywords = list(
        {
            *ls.localizations.get('scenario', []),
            *ls.localizations.get('scenario_outline', []),
            *ls.localizations.get('examples', []),
            *ls.keywords_any,
        }
    )
    ls.keywords.remove('*')

    for keyword in ls.steps:
        for value in ls.localizations.get(keyword, []):
            stripped_value = value.strip()
            if stripped_value in ['*']:
                continue

            ls.keywords.append(stripped_value)
