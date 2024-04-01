"""Core functionality of grizzly testdata."""
from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Set, Tuple, Type, cast

from grizzly.types import GrizzlyVariableType

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.context import GrizzlyContext
    from grizzly.types.locust import MessageHandler

    from .variables import AtomicVariable


__all__ = [
    'GrizzlyVariableType',
]


class GrizzlyVariables(dict):
    @classmethod
    def load_variable(cls, module_name: str, class_name: str) -> Type[AtomicVariable]:
        if module_name not in globals():
            module = import_module(module_name)
            globals()[class_name] = getattr(module, class_name)

        variable = globals()[class_name]
        return cast(Type['AtomicVariable'], variable)

    @classmethod
    def get_variable_spec(cls, name: str) -> Tuple[Optional[str], Optional[str], str, Optional[str]]:
        dot_count = name.count('.')

        if dot_count == 0 or 'Atomic' not in name:
            return None, None, name, None

        namespace: List[str] = []
        module_name: Optional[str] = None
        variable_type: Optional[str] = None
        variable_name: Optional[str] = None
        sub_variable_names: List[str] = []

        for part in name.split('.'):
            if part.startswith('Atomic'):
                variable_type = part
                continue

            if variable_type is None:
                namespace.append(part)
                continue

            if variable_name is None:
                variable_name = part
                continue

            sub_variable_names.append(part)

        module_name = 'grizzly.testdata.variables' if len(namespace) == 0 else '.'.join(namespace)

        sub_variable_name = '.'.join(sub_variable_names) if len(sub_variable_names) > 0 else None

        return module_name, variable_type, cast(str, variable_name), sub_variable_name

    @classmethod
    def get_initialization_value(cls, name: str) -> str:
        if name.count('.') > 1:
            variable_spec = GrizzlyVariables.get_variable_spec(name)
            name = '.'.join([part for part in variable_spec[:-1] if part is not None and part != 'grizzly.testdata.variables'])

        return name

    @classmethod
    def initialize_variable(cls, grizzly: GrizzlyContext, name: str) -> Tuple[Any, Set[str], Dict[str, MessageHandler]]:
        external_dependencies: Set[str] = set()
        message_handler: Dict[str, MessageHandler] = {}

        default_value = grizzly.state.variables.get(name, None)
        if default_value is None:
            message = f'variable "{name}" has not been declared'
            raise ValueError(message)

        module_name, variable_type, variable_name, _ = cls.get_variable_spec(name)

        if module_name is not None and variable_type is not None:
            variable = cls.load_variable(module_name, variable_type)
            external_dependencies = variable.__dependencies__
            message_handler = variable.__message_handlers__

            if getattr(variable, '__on_consumer__', False):
                value = cast(Any, '__on_consumer__')
            else:
                try:
                    value = variable(variable_name, default_value)
                except ValueError as e:
                    message = f'{name}: {default_value=}, exception={e!s}'
                    raise ValueError(message) from e
        else:
            value = default_value

        return value, external_dependencies, message_handler

    @classmethod
    def guess_datatype(cls, value: Any) -> GrizzlyVariableType:
        if isinstance(value, (int, bool, float)):
            return value

        check_value = value.replace('.', '', 1)
        casted_value: GrizzlyVariableType

        if check_value[0] == '-':
            check_value = check_value[1:]

        if check_value.isdecimal():
            casted_value = (str(value) if value.startswith('0') else int(float(value))) if float(value) % 1 == 0 else float(value)
        elif value.lower() in ['true', 'false']:
            casted_value = value.lower() == 'true'
        else:
            casted_value = str(value)
            if casted_value[0] in ['"', "'"]:
                if casted_value[0] != casted_value[-1] and casted_value.count(casted_value[0]) % 2 != 0:
                    message = f'{value} is incorrectly quoted'
                    raise ValueError(message)

                if casted_value[0] == casted_value[-1]:
                    casted_value = casted_value[1:-1]
            elif casted_value[-1] in ['"', "'"] and casted_value[-1] != casted_value[0] and casted_value.count(casted_value[-1]) % 2 != 0:
                message = f'{value} is incorrectly quoted'
                raise ValueError(message)

        return casted_value

    def __setitem__(self, key: str, value: GrizzlyVariableType) -> None:
        caster: Optional[Callable] = None

        module_name, variable_type, _, _ = self.get_variable_spec(key)

        if module_name is not None and variable_type is not None:
            try:
                variable = self.load_variable(module_name, variable_type)
                caster = variable.__base_type__
            except AttributeError:
                pass

        if isinstance(value, str):
            value = self.guess_datatype(value) if caster is None else caster(value)
        elif caster is not None:
            value = caster(value)

        super().__setitem__(key, value)
