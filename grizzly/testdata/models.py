from typing import Any, Callable, Optional

from .variables import load_variable
from ..types import TemplateDataType

class TemplateData(dict):
    @classmethod
    def guess_datatype(cls, value: Any) -> TemplateDataType:
        if isinstance(value, (int, bool, float)):
            return value

        check_value = value.replace('.', '', 1)
        casted_value: TemplateDataType

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

    def __setitem__(self, key: str, value: TemplateDataType) -> None:
        caster: Optional[Callable] = None

        if '.' in key:
            [name, _] = key.split('.', 1)
            try:
                variable = load_variable(name)
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
