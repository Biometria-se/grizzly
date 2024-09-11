"""Utilities related testdata."""
from __future__ import annotations

import logging
import re
from collections import namedtuple
from os import environ
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional, cast

from jinja2.meta import find_undeclared_variables

from grizzly.testdata.ast import get_template_variables
from grizzly.utils import has_template, is_file, merge_dicts, unflatten

from . import GrizzlyVariables

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.context import GrizzlyContext, GrizzlyContextScenario
    from grizzly.types import GrizzlyVariableType, TestdataType
    from grizzly.types.locust import MessageHandler


logger = logging.getLogger(__name__)

MAGIC_4 = 4


def initialize_testdata(grizzly: GrizzlyContext) -> tuple[TestdataType, set[str], dict[str, MessageHandler]]:
    """Create a structure of testdata per scenario."""
    testdata: TestdataType = {}
    template_variables = get_template_variables(grizzly)

    logger.debug('testdata: %r', template_variables)

    external_dependencies: set[str] = set()
    message_handlers: dict[str, MessageHandler] = {}

    for scenario, variables in template_variables.items():
        testdata[scenario.class_name] = {}
        initialized_datatypes: dict[str, Any] = {}

        for variable in variables:
            module_name, variable_type, variable_name, _ = GrizzlyVariables.get_variable_spec(variable)
            if module_name is not None and variable_type is not None:
                variable_datatype = f'{variable_type}.{variable_name}'
                if module_name != 'grizzly.testdata.variables':
                    variable_datatype = f'{module_name}.{variable_datatype}'
            else:
                variable_datatype = variable_name

            if variable_datatype not in initialized_datatypes:
                initialized_datatype, dependencies, message_handler = GrizzlyVariables.initialize_variable(scenario, variable_datatype)
                external_dependencies.update(dependencies)
                message_handlers.update(message_handler)
                initialized_datatypes.update({variable_datatype: initialized_datatype})

            testdata[scenario.class_name][variable] = initialized_datatypes[variable_datatype]

    return testdata, external_dependencies, message_handlers


def transform(scenario: GrizzlyContextScenario, data: dict[str, Any], *, objectify: Optional[bool] = True) -> dict[str, Any]:
    """Transform a dictionary with static values to something that can have values which are object."""
    testdata: dict[str, Any] = {}

    for key, value in data.items():
        module_name, variable_type, variable_name, _ = GrizzlyVariables.get_variable_spec(key)

        if '.' in key:
            if module_name is not None and variable_type is not None and value == '__on_consumer__':
                variable_type_instance = GrizzlyVariables.load_variable(module_name, variable_type)
                initial_value = scenario.variables.get(key, None)
                variable_instance = variable_type_instance.obtain(scenario=scenario, variable=variable_name, value=initial_value)

                try:
                    value = variable_instance[variable_name]  # noqa: PLW2901
                except Exception as e:
                    logger.exception('failed to get value from variable instance')

                    if scenario.failure_exception is not None:
                        raise scenario.failure_exception from e

            paths: list[str] = key.split('.')
            variable = paths.pop(0)
            struct = unflatten('.'.join(paths), value)

            if variable in testdata:
                testdata[variable] = merge_dicts(testdata[variable], struct)
            else:
                testdata[variable] = {**struct}
        else:
            testdata[key] = value

    if objectify:
        return _objectify(testdata)

    return testdata


def _objectify(testdata: dict[str, Any]) -> dict[str, Any]:
    for variable, attributes in testdata.items():
        if not isinstance(attributes, dict):
            continue

        attrs = _objectify(attributes)
        testdata[variable] = namedtuple('Testdata', attributes.keys())(**attrs)  # noqa: PYI024

    return testdata


def create_context_variable(scenario: GrizzlyContextScenario, variable: str, value: str) -> dict[str, Any]:
    """Create a variable as a context variable. Handles other separators than `.`."""
    if has_template(value):
        scenario.orphan_templates.append(value)

    casted_value = resolve_variable(scenario, value)
    casted_variable = cast(str, resolve_variable(scenario, variable))

    prefix: Optional[str] = None

    if casted_variable.count('/') == 1 and casted_variable.count('.') > 0:
        prefix, casted_variable = casted_variable.split('/', 1)

    casted_variable = casted_variable.lower().replace(' ', '_').replace('/', '.')

    transformed = transform(scenario, {casted_variable: casted_value}, objectify=False)

    if prefix is not None:
        transformed = {prefix: transformed}

    return transformed


def resolve_template(scenario: GrizzlyContextScenario, value: str) -> str:
    template = scenario.jinja2.from_string(value)
    template_parsed = template.environment.parse(value)
    template_variables = find_undeclared_variables(template_parsed)

    for template_variable in template_variables:
        if f'{template_variable} is defined' in value or f'{template_variable} is not defined' in value:
            continue

        assert template_variable in scenario.variables, f'value contained variable "{template_variable}" which has not been declared'

    return template.render(**scenario.variables)


def resolve_parameters(scenario: GrizzlyContextScenario, value: str) -> str:
    regex = r"\$(conf|env)::([^\$]+)\$"

    matches = re.finditer(regex, value, re.MULTILINE)

    for match in matches:
        match_type = match.group(1)
        variable_name = match.group(2)

        if match_type == 'conf':
            assert variable_name in scenario.grizzly.state.configuration, f'configuration variable "{variable_name}" is not set'
            variable_value = scenario.grizzly.state.configuration[variable_name]
        elif match_type == 'env':
            assert variable_name in environ, f'environment variable "{variable_name}" is not set'
            variable_value = environ.get(variable_name, None)

        if not isinstance(variable_value, str):
            variable_value = str(variable_value)

        value = value.replace(f'${match_type}::{variable_name}$', variable_value)

    if len(value) > MAGIC_4 and value[0] == '$' and value[1] != '.':  # $. is jsonpath expression...
        message = f'"{value}" is not a correctly specified templating variable, variables must match "$(conf|env)::<variable name>$"'
        raise ValueError(message)

    return value

def read_file(value: str) -> str:
    base_dir = environ.get('GRIZZLY_CONTEXT_ROOT', None)

    if base_dir is None or len(value.strip()) < 1:
        return value

    try:
        file = Path(base_dir) / 'requests' / value
        return file.read_text()
    except (OSError, FileNotFoundError):
        return value


def resolve_variable(
        scenario: GrizzlyContextScenario, value: str, *, guess_datatype: bool = True, try_template: bool = True, try_file: bool = True,
) -> GrizzlyVariableType:
    """Resolve a value to its actual value, since it can be a jinja2 template or any dollar reference. Return type can be actual type of the value."""
    if len(value) < 1:
        return value

    # if variable value starts with `file://`, the file should be read
    if len(value) > 7 and value[:7] == 'file://' and not try_file:
        try_file = True  # explicitly stated as a file, we should read its contents
        try_template = False  # but not try to render any variables...
        value = value[7:]

        if value[0] == '/':
            value = value[1:]

        if value[:2] == './':
            value = value[2:]

    quote_char: Optional[str] = None
    if value[0] in ['"', "'"] and value[0] == value[-1]:
        quote_char = value[0]
        value = value[1:-1]

    if try_file and is_file(value):
        value = read_file(value)

    if try_template and has_template(value):
        value = resolve_template(scenario, value)

    if '$conf' in value or '$env' in value:
        value = resolve_parameters(scenario, value)

    if guess_datatype:
        resolved_variable = GrizzlyVariables.guess_datatype(value)
    elif quote_char is not None and value.count(' ') > 0:
        resolved_variable = f'{quote_char}{value}{quote_char}'
    else:
        resolved_variable = value

    return resolved_variable


