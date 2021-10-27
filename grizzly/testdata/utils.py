from typing import Optional, List, Dict, Any, Tuple, Set

from ..context import GrizzlyContext
from ..task import RequestTask
from ..types import TestdataType
from .variables import load_variable
from .ast import get_template_variables


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
        variable = load_variable(variable_type)
        external_dependencies = variable.__dependencies__
        value = variable(variable_name, default_value)
    else:
        value = default_value

    return value, external_dependencies
