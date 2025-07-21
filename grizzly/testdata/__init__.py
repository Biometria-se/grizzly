"""Core functionality of grizzly testdata."""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Callable

    from grizzly.context import GrizzlyContextScenario
    from grizzly.testdata.communication import GrizzlyDependencies
    from grizzly.types import GrizzlyVariableType

    from .variables import AtomicVariable


class GrizzlyVariables(dict):
    _alias: dict[str, str]
    _persistent: dict[str, str]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)

        self._alias = {}
        self._persistent = {}

    @property
    def alias(self) -> dict[str, str]:
        return self._alias

    @property
    def persistent(self) -> dict[str, str]:
        return self._persistent

    @classmethod
    def load_variable(cls, module_name: str, class_name: str) -> type[AtomicVariable]:
        if module_name not in globals():
            module = import_module(module_name)
            globals()[class_name] = getattr(module, class_name)

        variable = globals()[class_name]
        return cast('type[AtomicVariable]', variable)

    @classmethod
    def get_variable_spec(cls, name: str) -> tuple[str | None, str | None, str, str | None]:
        dot_count = name.count('.')

        if dot_count == 0 or 'Atomic' not in name:
            return None, None, name, None

        namespace: list[str] = []
        module_name: str | None = None
        variable_type: str | None = None
        variable_name: str | None = None
        sub_variable_names: list[str] = []

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

        return module_name, variable_type, cast('str', variable_name), sub_variable_name

    @classmethod
    def get_initialization_value(cls, name: str) -> str:
        if name.count('.') > 1:
            variable_spec = GrizzlyVariables.get_variable_spec(name)
            name = '.'.join([part for part in variable_spec[:-1] if part is not None and part != 'grizzly.testdata.variables'])

        return name

    @classmethod
    def initialize_variable(cls, scenario: GrizzlyContextScenario, name: str) -> tuple[Any, GrizzlyDependencies]:
        dependencies: GrizzlyDependencies = set()

        default_value = scenario.variables.get(name, None)
        if default_value is None:
            message = f'variable "{name}" has not been declared'
            raise ValueError(message)

        module_name, variable_type, variable_name, _ = cls.get_variable_spec(name)

        if module_name is not None and variable_type is not None:
            variable = cls.load_variable(module_name, variable_type)
            dependencies = variable.__dependencies__

            if getattr(variable, '__on_consumer__', False):
                value = cast('Any', '__on_consumer__')
            else:
                try:
                    value = variable(scenario=scenario, variable=variable_name, value=default_value)
                except ValueError as e:
                    message = f'{name}: {default_value=}, exception={e!s}'
                    raise ValueError(message) from e
        else:
            value = default_value

        return value, dependencies

    @classmethod
    def guess_datatype(cls, value: Any) -> GrizzlyVariableType:
        if isinstance(value, int | bool | float) or (isinstance(value, str) and len(value) == 0):
            return value

        check_value = value.replace('.', '', 1)
        casted_value: GrizzlyVariableType

        if check_value[0] == '-':
            check_value = check_value[1:]

        if check_value.isdecimal():
            casted_value = (str(value) if value.startswith('0') and len(value) > 1 else int(float(value))) if float(value) % 1 == 0 else float(value)
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

    def update(self, *args: Any, **kwargs: Any) -> None:
        if len(args) == 1 and isinstance(args[0], dict):
            for key, value in args[0].items():
                self.__setitem__(key, value)
        else:
            super().update(*args, **kwargs)

    def __setitem__(self, key: str, value: GrizzlyVariableType) -> None:
        caster: Callable | None = None

        # only when initializing
        if key not in self:
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
