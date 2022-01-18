import logging

from typing import Optional, List, Dict, Any, Tuple, Set, cast
from collections import namedtuple
from os import environ
from time import perf_counter as time

from jinja2 import Template, Environment
from jinja2.meta import find_undeclared_variables
from locust.exception import StopUser

from ..context import GrizzlyContext
from ..task import RequestTask
from ..types import TestdataType, GrizzlyDictValueType, GrizzlyDict
from ..utils import merge_dicts
from .ast import get_template_variables


logger = logging.getLogger(__name__)


def initialize_testdata(sources: Optional[List[RequestTask]]) -> Tuple[TestdataType, Set[str]]:
    testdata: TestdataType = {}
    template_variables = get_template_variables(sources)

    initialized_datatypes: Dict[str, Any] = {}
    external_dependencies: Set[str] = set()

    for scenario, variables in template_variables.items():
        testdata[scenario] = {}

        for variable in variables:
            if variable.count('.') > 1:
                variable_datatype = '.'.join(variable.split('.')[0:2])
            else:
                variable_datatype = variable

            if variable_datatype not in initialized_datatypes:
                initialized_datatypes[variable_datatype], dependencies = _get_variable_value(variable_datatype)
                external_dependencies.update(dependencies)

            testdata[scenario][variable] = initialized_datatypes[variable_datatype]

    return testdata, external_dependencies


def _get_variable_value(name: str) -> Tuple[Any, Set[str]]:
    grizzly = GrizzlyContext()
    default_value = grizzly.state.variables.get(name, None)
    external_dependencies: Set[str] = set()

    if '.' in name:
        [variable_type, variable_name] = name.split('.', 1)
        variable = GrizzlyDict.load_variable(variable_type)
        external_dependencies = variable.__dependencies__
        if getattr(variable, '__on_consumer__', False):
            value = cast(Any, '__on_consumer__')
        else:
            value = variable(variable_name, default_value)
    else:
        value = default_value

    return value, external_dependencies


def transform(data: Dict[str, Any], objectify: Optional[bool] = True) -> Dict[str, Any]:
    testdata: Dict[str, Any] = {}

    for key, value in data.items():
        if '.' in key:
            if value == '__on_consumer__':
                grizzly = GrizzlyContext()
                [variable_type, variable_name] = key.split('.', 1)
                variable_class_type = GrizzlyDict.load_variable(variable_type)
                try:
                    initial_value = grizzly.state.variables.get(key, None)
                    variable_instance = variable_class_type(variable_name, initial_value)
                except ValueError as e:
                    if 'object already has attribute' in str(e):
                        variable_instance = variable_class_type.get()
                    else:
                        raise e

                start_time = time()
                exception: Optional[Exception] = None
                try:
                    value = variable_instance[variable_name]
                except Exception as e:
                    exception = e
                    logger.error(str(e), exc_info=grizzly.state.verbose)
                finally:
                    response_time = int((time() - start_time) * 1000)
                    grizzly.state.environment.events.request.fire(
                        request_type='VAR ',
                        name=key,
                        response_time=response_time,
                        response_length=0,
                        context=None,
                        exception=exception,
                    )

                if exception is not None:
                    raise StopUser()

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
    else:
        return testdata


def _objectify(testdata: Dict[str, Any]) -> Dict[str, Any]:
    for variable, attributes in testdata.items():
        if not isinstance(attributes, dict):
            continue

        attributes = _objectify(attributes)
        testdata[variable] = namedtuple('Testdata', attributes.keys())(**attributes)

    return testdata


def create_context_variable(grizzly: GrizzlyContext, variable: str, value: str) -> Dict[str, Any]:
    casted_value = resolve_variable(grizzly, value)

    variable = variable.lower().replace(' ', '_').replace('/', '.')

    return transform({variable: casted_value}, objectify=False)


def resolve_variable(grizzly: GrizzlyContext, value: str, guess_datatype: Optional[bool] = True) -> GrizzlyDictValueType:
    if len(value) < 1:
        return value

    quote_char: Optional[str] = None
    if value[0] in ['"', "'"] and value[0] == value[-1]:
        quote_char = value[0]
        value = value[1:-1]

    resolved_variable: GrizzlyDictValueType
    if '{{' in value and '}}' in value:
        template = Template(value)
        j2env = Environment(autoescape=False)
        template_parsed = j2env.parse(value)
        template_variables = find_undeclared_variables(template_parsed)

        for template_variable in template_variables:
            assert template_variable in grizzly.state.variables, f'value contained variable "{template_variable}" which has not been set'

        resolved_variable = template.render(**grizzly.state.variables)
    elif len(value) > 4 and value[0] == '$' and value[1] != '.':  # $. is jsonpath expression...
        if value[0:5] == '$conf':
            variable = value[7:]
            assert variable in grizzly.state.configuration, f'configuration variable "{variable}" is not set'
            resolved_variable = grizzly.state.configuration[variable]
        elif value[0:4] == '$env':
            variable = value[6:]
            env_value = environ.get(variable, None)
            assert env_value is not None, f'environment variable "{variable}" is not set'
            resolved_variable = env_value
        else:
            raise ValueError(f'{value.split("::", 1)[0]} is not implemented')
    else:
        resolved_variable = value

    if guess_datatype:
        resolved_variable = GrizzlyDict.guess_datatype(resolved_variable)
    elif quote_char is not None and isinstance(resolved_variable, str) and resolved_variable.count(' ') > 0:
        resolved_variable = f'{quote_char}{resolved_variable}{quote_char}'


    return resolved_variable
