import pkgutil
import inspect

from os import environ, path
from typing import Generator, Callable

import pytest

import grizzly.testdata.variables as variables
from grizzly.context import GrizzlyContext


@pytest.fixture
def cleanup() -> Generator[Callable, None, None]:
    def noop() -> None:
        return

    yield noop

    try:
        GrizzlyContext.destroy()
    except:
        pass

    # automagically find all Atomic variables and try to destroy them, instead of explicitlly define them one by one
    for _, package_name, _ in pkgutil.iter_modules([path.dirname(variables.__file__)]):
        module = getattr(variables, package_name)
        for member_name, member in inspect.getmembers(module):
            if inspect.isclass(member) and member_name.startswith('Atomic') and member_name != 'AtomicVariable':
                destroy = getattr(member, 'destroy', None)
                if destroy is None:
                    continue

                try:
                    destroy()
                except:
                    pass

    try:
        del environ['GRIZZLY_CONTEXT_ROOT']
    except KeyError:
        pass
