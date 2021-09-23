from os import environ
from typing import Generator, Callable

import pytest

from grizzly.testdata.variables import AtomicCsvRow, AtomicDirectoryContents, AtomicIntegerIncrementer, AtomicInteger, AtomicDate, AtomicRandomInteger, AtomicCsvRow
from grizzly.context import LocustContextSetup, LocustContext


@pytest.fixture
def cleanup() -> Generator[Callable, None, None]:
    def noop() -> None:
        return

    yield noop

    try:
        LocustContext.destroy()
    except ValueError:
        pass

    try:
        AtomicInteger.destroy()
    except Exception:
        pass

    try:
        AtomicIntegerIncrementer.destroy()
    except Exception:
        pass

    try:
        AtomicDate.destroy()
    except Exception:
        pass

    try:
        AtomicDirectoryContents.destroy()
    except Exception:
        pass

    try:
        AtomicRandomInteger.destroy()
    except Exception:
        pass

    try:
        AtomicCsvRow.destroy()
    except Exception:
        pass

    try:
        del environ['LOCUST_CONTEXT_ROOT']
    except KeyError:
        pass
