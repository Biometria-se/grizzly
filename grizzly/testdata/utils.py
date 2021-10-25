from typing import Optional, List, Dict, Any

from ..context import GrizzlyContext
from ..task import RequestTask
from ..types import TestdataType
from .variables import load_variable
from .ast import get_template_variables


def initialize_testdata(sources: Optional[List[RequestTask]]) -> TestdataType:
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


def _get_variable_value(name: str) -> Any:
    grizzly = GrizzlyContext()
    default_value = grizzly.state.variables.get(name, None)

    if '.' in name:
        [variable_type, variable_name] = name.split('.', 1)
        value = load_variable(variable_type)(variable_name, default_value)
    else:
        value = default_value

    return value
