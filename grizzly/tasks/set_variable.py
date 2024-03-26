"""@anchor pydoc:grizzly.tasks.set_variable Set variable
This task sets a testdata variable during runtime.

## Step implementations

* {@pylink grizzly.steps.setup.step_setup_variable_value}

## Statistics

This task does not have any request statistics entries.

## Arguments

* `variable` _str_ - name of the variable that should be set

* `value` _value_ - value of the variable being set, must be a template
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, MutableMapping, Optional, Type, cast

from grizzly.testdata import GrizzlyVariables
from grizzly.testdata.utils import create_context_variable, read_file
from grizzly.types import VariableType
from grizzly.utils import has_template, is_file

from . import GrizzlyTask, grizzlytask, template

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.scenarios import GrizzlyScenario
    from grizzly.testdata.variables import AtomicVariable


@template('variable_template', 'value')
class SetVariableTask(GrizzlyTask):
    variable: str
    value: str
    variable_type: VariableType

    _variable_instance: Optional[MutableMapping[str, Any]] = None
    _variable_instance_type: Optional[Type[AtomicVariable]] = None
    _variable_key: str

    def __init__(self, variable: str, value: str, variable_type: VariableType) -> None:
        super().__init__()

        self.variable = variable
        self.value = value
        self.variable_type = variable_type

        if variable_type == VariableType.VARIABLES:
            module_name, variable_type_name, variable, sub_variable = GrizzlyVariables.get_variable_spec(self.variable)

            if not (module_name is None or variable_type_name is None):
                self._variable_instance_type = GrizzlyVariables.load_variable(module_name, variable_type_name)

                if not getattr(self._variable_instance_type, '__settable__', False):
                    message = f'{module_name}.{variable_type_name} is not settable'
                    raise AttributeError(message)
        else:
            sub_variable = None

        if sub_variable is not None:
            self._variable_key = f'{variable}.{sub_variable}'
        else:
            self._variable_key = variable

    @property
    def variable_template(self) -> str:
        """Create a dummy template for the variable, used so we will not complain that this variable isn't used anywhere."""
        if not has_template(self.variable) and self.variable_type == VariableType.VARIABLES:
            return f'{{{{ {self.variable} }}}}'

        return self.variable

    def __call__(self) -> grizzlytask:
        @grizzlytask
        def task(parent: GrizzlyScenario) -> Any:
            value = parent.render(self.value)

            if is_file(value):
                value = parent.render(read_file(value))

            if self.variable_type == VariableType.VARIABLES:
                if self._variable_instance is None and self._variable_instance_type is not None:
                    self._variable_instance = cast(MutableMapping[str, Any], self._variable_instance_type.get())


                if self._variable_instance is not None:
                    self._variable_instance[self._variable_key] = value

                # always update user context with new value
                parent.user._context['variables'][self.variable] = value
            else:
                parent.user.add_context(create_context_variable(self.grizzly, self.variable, value, scenario=parent.user._scenario))

        return task
