"""Utilities related testdata."""
from __future__ import annotations

import itertools
import logging
import re
from collections import namedtuple
from os import environ
from time import perf_counter as time
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Set, Tuple, cast

from jinja2.filters import FILTERS
from jinja2.meta import find_undeclared_variables

from grizzly.testdata.ast import get_template_variables
from grizzly.types import GrizzlyVariableType, RequestType, TestdataType
from grizzly.types.locust import MessageHandler, StopUser
from grizzly.utils import is_template, merge_dicts

from . import GrizzlyVariables

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.context import GrizzlyContext, GrizzlyContextScenario


logger = logging.getLogger(__name__)

MAGIC_4 = 4


def initialize_testdata(grizzly: GrizzlyContext) -> Tuple[TestdataType, Set[str], Dict[str, MessageHandler]]:
    """Create a structure of testdata per scenario."""
    testdata: TestdataType = {}
    template_variables = get_template_variables(grizzly)

    found_variables = set()
    for variable in itertools.chain(*template_variables.values()):
        module_name, variable_type, variable_name, _ = GrizzlyVariables.get_variable_spec(variable)

        if module_name is None and variable_type is None:
            found_variables.add(variable_name)
        else:
            prefix = f'{module_name}.' if module_name != 'grizzly.testdata.variables' else ''
            found_variables.add(f'{prefix}{variable_type}.{variable_name}')

    declared_variables = set(grizzly.state.variables.keys())

    # check except between declared variables and variables found in templates
    missing_in_templates = [variable for variable in declared_variables if variable not in found_variables]
    assert len(missing_in_templates) == 0, f'variables has been declared, but cannot be found in templates: {",".join(missing_in_templates)}'

    missing_declarations = [variable for variable in found_variables if variable not in declared_variables]
    assert len(missing_declarations) == 0, f'variables has been found in templates, but have not been declared: {",".join(missing_declarations)}'

    initialized_datatypes: Dict[str, Any] = {}
    external_dependencies: Set[str] = set()
    message_handlers: Dict[str, MessageHandler] = {}

    for scenario, variables in template_variables.items():
        testdata[scenario] = {}

        for variable in variables:
            module_name, variable_type, variable_name, _ = GrizzlyVariables.get_variable_spec(variable)
            if module_name is not None and variable_type is not None:
                variable_datatype = f'{variable_type}.{variable_name}'
                if module_name != 'grizzly.testdata.variables':
                    variable_datatype = f'{module_name}.{variable_datatype}'
            else:
                variable_datatype = variable_name

            if variable_datatype not in initialized_datatypes:
                initialized_datatypes[variable_datatype], dependencies, message_handler = GrizzlyVariables.initialize_variable(grizzly, variable_datatype)
                external_dependencies.update(dependencies)
                message_handlers.update(message_handler)

            testdata[scenario][variable] = initialized_datatypes[variable_datatype]

    return testdata, external_dependencies, message_handlers


def transform(grizzly: GrizzlyContext, data: Dict[str, Any], scenario: Optional[GrizzlyContextScenario] = None, *, objectify: Optional[bool] = True) -> Dict[str, Any]:
    """Transform a dictionary with static values to something that can have values which are object."""
    testdata: Dict[str, Any] = {}

    for key, value in data.items():
        module_name, variable_type, variable_name, _ = GrizzlyVariables.get_variable_spec(key)

        if '.' in key:
            if module_name is not None and variable_type is not None and value == '__on_consumer__':
                variable_type_instance = GrizzlyVariables.load_variable(module_name, variable_type)
                initial_value = grizzly.state.variables.get(key, None)
                variable_instance = variable_type_instance.obtain(variable_name, initial_value)

                start_time = time()
                exception: Optional[Exception] = None
                try:
                    value = variable_instance[variable_name]  # noqa: PLW2901
                except Exception as e:
                    exception = e
                    logger.exception('failed to get value from variable instance')
                finally:
                    response_time = int((time() - start_time) * 1000)
                    if scenario is not None:
                        grizzly.state.locust.environment.events.request.fire(
                            request_type=RequestType.VARIABLE(),
                            name=f'{scenario.identifier} {key}',
                            response_time=response_time,
                            response_length=0,
                            context=None,
                            exception=exception,
                        )

                if exception is not None:
                    if scenario is None:
                        raise StopUser
                    elif scenario.failure_exception is not None:  # noqa: RET506
                        raise scenario.failure_exception

            paths: List[str] = key.split('.')
            variable = paths.pop(0)
            path = paths.pop()
            struct = {path: value}
            paths.reverse()

            for path in paths:
                struct = {path: {**struct}}

            if variable in testdata:
                testdata[variable] = merge_dicts(testdata[variable], struct)
            else:
                testdata[variable] = {**struct}
        else:
            testdata[key] = value

    if objectify:
        return _objectify(testdata)

    return testdata


def _objectify(testdata: Dict[str, Any]) -> Dict[str, Any]:
    for variable, attributes in testdata.items():
        if not isinstance(attributes, dict):
            continue

        attrs = _objectify(attributes)
        testdata[variable] = namedtuple('Testdata', attributes.keys())(**attrs)  # noqa: PYI024

    return testdata


def create_context_variable(grizzly: GrizzlyContext, variable: str, value: str) -> Dict[str, Any]:
    """Create a variable as a context variable. Handles other separators than `.`."""
    if is_template(value):
        grizzly.scenario.orphan_templates.append(value)

    casted_value = resolve_variable(grizzly, value)
    casted_variable = cast(str, resolve_variable(grizzly, variable))

    prefix: Optional[str] = None

    if casted_variable.count('/') == 1 and casted_variable.count('.') > 0:
        prefix, casted_variable = casted_variable.split('/', 1)

    casted_variable = casted_variable.lower().replace(' ', '_').replace('/', '.')

    transformed = transform(grizzly, {casted_variable: casted_value}, objectify=False)

    if prefix is not None:
        transformed = {prefix: transformed}

    return transformed


def _resolve_template(grizzly: GrizzlyContext, value: str) -> str:
    template = grizzly.state.jinja2.from_string(value)
    template_parsed = template.environment.parse(value)
    template_variables = find_undeclared_variables(template_parsed)

    for template_variable in template_variables:
        assert template_variable in grizzly.state.variables, f'value contained variable "{template_variable}" which has not been declared'

    return template.render(**grizzly.state.variables)

def _resolve_dollar_path(grizzly: GrizzlyContext, value: str) -> str:
    regex = r"\$(conf|env)::([^\$]+)\$"

    matches = re.finditer(regex, value, re.MULTILINE)

    for match in matches:
        match_type = match.group(1)
        variable_name = match.group(2)

        if match_type == 'conf':
            assert variable_name in grizzly.state.configuration, f'configuration variable "{variable_name}" is not set'
            variable_value = grizzly.state.configuration[variable_name]
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


def resolve_variable(grizzly: GrizzlyContext, value: str, *, guess_datatype: Optional[bool] = True, only_grizzly: bool = False) -> GrizzlyVariableType:
    """Resolve a value to its actual value, since it can be a jinja2 template or any dollar reference. Return type can be actual type of the value."""
    if len(value) < 1:
        return value

    quote_char: Optional[str] = None
    if value[0] in ['"', "'"] and value[0] == value[-1]:
        quote_char = value[0]
        value = value[1:-1]

    resolved_variable: GrizzlyVariableType
    if is_template(value) and not only_grizzly:
        resolved_variable = _resolve_template(grizzly, value)
    elif '$conf' in value or '$env' in value:
        resolved_variable = _resolve_dollar_path(grizzly, value)
    else:
        resolved_variable = value

    if guess_datatype:
        resolved_variable = GrizzlyVariables.guess_datatype(resolved_variable)
    elif quote_char is not None and isinstance(resolved_variable, str) and resolved_variable.count(' ') > 0:
        resolved_variable = f'{quote_char}{resolved_variable}{quote_char}'

    return resolved_variable


class templatingfilter:
    name: str

    def __init__(self, func: Callable) -> None:
        name = func.__name__
        existing_filter = FILTERS.get(name, None)

        if existing_filter is None:
            FILTERS[name] = func
        elif existing_filter is not func:
            message = f'{name} is already registered as a filter'
            raise AssertionError(message)
        else:
            # code executed twice, so adding the same filter again
            pass
