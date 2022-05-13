from typing import TYPE_CHECKING, Type, Any, Optional, Callable, List, Tuple, Set, cast
from importlib import import_module

from ..types import GrizzlyVariableType


if TYPE_CHECKING:
    from ..context import GrizzlyContext
    from .variables import AtomicVariable


__all__ = [
    'GrizzlyVariableType',
]


class GrizzlyVariables(dict):
    @classmethod
    def load_variable(cls, module_name: str, class_name: str) -> Type['AtomicVariable']:
        if module_name not in globals():
            module = import_module(module_name)
            globals()[class_name] = getattr(module, class_name)

        variable = globals()[class_name]
        return cast(Type['AtomicVariable'], variable)

    @classmethod
    def get_variable_spec(cls, name: str) -> Tuple[Optional[str], Optional[str], str]:
        dot_count = name.count('.')

        if dot_count == 0 or 'Atomic' not in name:
            return None, None, name
        else:
            namespace: List[str] = []
            module_name: Optional[str] = None
            variable_type: Optional[str] = None
            variable_name: Optional[str] = None

            for part in name.split('.'):
                if part.startswith('Atomic'):
                    variable_type = part
                    continue

                if variable_type is None:
                    namespace.append(part)
                    continue
                else:
                    variable_name = part
                    break

            if len(namespace) == 0:
                module_name = 'grizzly.testdata.variables'
            else:
                module_name = '.'.join(namespace)

            return module_name, variable_type, cast(str, variable_name)

    @classmethod
    def get_variable_value(cls, grizzly: 'GrizzlyContext', name: str) -> Tuple[Any, Set[str]]:
        external_dependencies: Set[str] = set()

        default_value = grizzly.state.variables.get(name, None)
        module_name, variable_type, variable_name = cls.get_variable_spec(name)

        if module_name is not None and variable_type is not None:
            variable = cls.load_variable(module_name, variable_type)
            external_dependencies = variable.__dependencies__
            if getattr(variable, '__on_consumer__', False):
                value = cast(Any, '__on_consumer__')
            else:
                try:
                    value = variable(variable_name, default_value)
                except ValueError as e:
                    raise ValueError(f'{name}: {default_value=}, exception={str(e)}') from e
        else:
            value = default_value

        return value, external_dependencies

    @classmethod
    def guess_datatype(cls, value: Any) -> GrizzlyVariableType:
        if isinstance(value, (int, bool, float)):
            return value

        check_value = value.replace('.', '', 1)
        casted_value: GrizzlyVariableType

        if check_value[0] == '-':
            check_value = check_value[1:]

        if check_value.isdecimal():
            if float(value) % 1 == 0:
                if value.startswith('0'):
                    casted_value = str(value)
                else:
                    casted_value = int(float(value))
            else:
                casted_value = float(value)
        elif value.lower() in ['true', 'false']:
            casted_value = value.lower() == 'true'
        else:
            casted_value = str(value)
            if casted_value[0] in ['"', "'"]:
                if casted_value[0] != casted_value[-1] and casted_value.count(casted_value[0]) % 2 != 0:
                    raise ValueError(f'{value} is incorrectly quoted')

                if casted_value[0] == casted_value[-1]:
                    casted_value = casted_value[1:-1]
            elif casted_value[-1] in ['"', "'"] and casted_value[-1] != casted_value[0] and casted_value.count(casted_value[-1]) % 2 != 0:
                raise ValueError(f'{value} is incorrectly quoted')

        return casted_value

    def __setitem__(self, key: str, value: GrizzlyVariableType) -> None:
        caster: Optional[Callable] = None

        module_name, variable_type, _ = self.get_variable_spec(key)
        if module_name is not None and variable_type is not None:
            try:
                variable = self.load_variable(module_name, variable_type)
                caster = variable.__base_type__
            except AttributeError:
                pass

        if isinstance(value, str):
            if caster is None:
                value = self.guess_datatype(value)
            else:
                value = caster(value)
        elif caster is not None:
            value = caster(value)

        super().__setitem__(key, value)
