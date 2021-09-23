from typing import Optional, List, Dict, Any
from collections import namedtuple
from collections.abc import Mapping
from copy import deepcopy

from ..context import LocustContext, RequestContext
from ..types import TestdataType
from .variables import load_variable
from .ast import get_template_variables


def initialize_testdata(sources: Optional[List[RequestContext]]) -> TestdataType:
    testdata: TestdataType = {}
    template_variables = get_template_variables(sources)

    initialized_datatypes: Dict[str, Any] = {}

    for scenario, variables in template_variables.items():
        testdata[scenario] = {}

        for variable in variables:
            if variable.count('.') > 1:
                variable_datatype = '.'.join(variable.split('.')[0:2])
            else:
                variable_datatype = variable

            if variable_datatype not in initialized_datatypes:
                initialized_datatypes[variable_datatype] = _get_variable_value(variable_datatype)

            testdata[scenario][variable] = initialized_datatypes[variable_datatype]

    return testdata


def merge_dicts(merged: Dict[str, Any], source: Dict[str, Any]) -> Dict[str, Any]:
    merged = deepcopy(merged)
    source = deepcopy(source)

    for k in source.keys():
        if (k in merged and isinstance(merged[k], dict)
                and isinstance(source[k], Mapping)):
            merged[k] = merge_dicts(merged[k], source[k])
        else:
            merged[k] = source[k]

    return merged


def transform(data: Dict[str, Any], raw: Optional[bool] = False) -> Dict[str, Any]:
    testdata: Dict[str, Any] = {}

    for key, value in data.items():
        if '.' in key:
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

    if not raw:
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


def _get_variable_value(name: str) -> Any:
    locust_context = LocustContext()
    default_value = locust_context.state.variables.get(name, None)

    if '.' in name:
        [variable_type, variable_name] = name.split('.', 1)
        value = load_variable(variable_type)(variable_name, default_value)
    else:
        value = default_value

    return value
