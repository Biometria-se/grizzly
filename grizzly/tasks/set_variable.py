'''
@anchor pydoc:grizzly.tasks.set_variable Set variable
This task sets a testdata variable during runtime.

## Step implementations

* {@pylink grizzly.steps.setup.step_setup_variable_value}

## Statistics

This task does not have any request statistics entries.

## Arguments

* `variable` _str_ - name of the variable that should be set

* `value` _value_ - value of the variable being set, must be a template
'''
from __future__ import annotations
from typing import TYPE_CHECKING, Any, Optional, MutableMapping, Type, cast

from grizzly.testdata import GrizzlyVariables
from grizzly.testdata.variables import AtomicVariable

from . import GrizzlyTask, template, grizzlytask

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.scenarios import GrizzlyScenario


@template('variable_template', 'value')
class SetVariableTask(GrizzlyTask):
    variable: str
    value: str

    _variable_instance: Optional[MutableMapping[str, Any]] = None
    _variable_instance_type: Optional[Type[AtomicVariable]] = None
    _variable_key: str

    def __init__(self, variable: str, value: str) -> None:
        super().__init__()

        self.variable = variable
        self.value = value

        module_name, variable_type, variable, sub_variable = GrizzlyVariables.get_variable_spec(self.variable)

        if not (module_name is None or variable_type is None):
            self._variable_instance_type = GrizzlyVariables.load_variable(module_name, variable_type)

            if not getattr(self._variable_instance_type, '__settable__', False):
                raise AttributeError(f'{module_name}.{variable_type} is not settable')

        if sub_variable is not None:
            self._variable_key = f'{variable}.{sub_variable}'
        else:
            self._variable_key = variable

    @property
    def variable_template(self) -> str:
        if not ('{{' in self.variable and '}}' in self.variable):
            return f'{{{{ {self.variable} }}}}'

        return self.variable

    def __call__(self) -> grizzlytask:
        @grizzlytask
        def task(parent: GrizzlyScenario) -> Any:
            if self._variable_instance is None and self._variable_instance_type is not None:
                self._variable_instance = cast(MutableMapping[str, Any], self._variable_instance_type.get())

            value = parent.render(self.value)

            if self._variable_instance is not None:
                self._variable_instance[self._variable_key] = value

            # always update user context with new value
            parent.user._context['variables'][self.variable] = value

        return task
