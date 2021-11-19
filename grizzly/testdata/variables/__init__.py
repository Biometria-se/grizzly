'''This package contains special variables that can be used in a feature file and is synchronized between locust workers.'''
from typing import Type, cast

from importlib import import_module

from ...types import AtomicVariable


def load_variable(name: str) -> Type[AtomicVariable]:
    if name not in globals():
        module = import_module(__name__)
        globals()[name] = getattr(module, name)

    variable = globals()[name]
    return cast(Type[AtomicVariable], variable)


def destroy_variables() -> None:
    for name in globals().keys():
        if not (name.startswith('Atomic') and not name == 'AtomicVariable'):
            continue

        module = globals()[name]
        if issubclass(module, AtomicVariable):
            try:
                module.destroy()
            except ValueError:
                pass

    if 'AtomicVariable' in globals().keys():
        try:
            AtomicVariable.destroy()
        except ValueError:
            pass


from .random_integer import AtomicRandomInteger
from .integer_incrementer import AtomicIntegerIncrementer
from .date import AtomicDate
from .directory_contents import AtomicDirectoryContents
from .csv_row import AtomicCsvRow
from .random_string import AtomicRandomString
from .messagequeue import AtomicMessageQueue
from .servicebus import AtomicServiceBus

__all__ = [
    'AtomicRandomInteger',
    'AtomicIntegerIncrementer',
    'AtomicDate',
    'AtomicDirectoryContents',
    'AtomicCsvRow',
    'AtomicRandomString',
    'AtomicMessageQueue',
    'AtomicServiceBus',
    'load_variable',
    'destroy_variables',
]


