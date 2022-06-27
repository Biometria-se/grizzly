
from abc import ABCMeta
from enum import Enum, EnumMeta
from typing import Tuple, Any, Dict, Optional


class PermutationMeta(type, metaclass=ABCMeta):
    _vector: Optional[Tuple[bool, bool]]

    def __new__(mcs, name: str, bases: Tuple[Any, ...], attributes: Dict[str, Any], vector: Optional[Tuple[bool, bool]] = None) -> 'PermutationMeta':
        class_instance = super().__new__(mcs, name, bases, attributes)
        if vector is not None or not hasattr(class_instance, '_vector'):
            class_instance._vector = vector

        return class_instance


class PermutationEnumMeta(EnumMeta, PermutationMeta):
    pass


class PermutationEnum(Enum, metaclass=PermutationEnumMeta):
    @classmethod
    @property
    def vector(cls) -> Optional[Tuple[bool, bool]]:
        return cls._vector
