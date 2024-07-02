"""Unit tests of grizzly.steps.scenario.tasks.write_file."""
from __future__ import annotations

from base64 import b64encode
from contextlib import suppress
from os import environ
from typing import TYPE_CHECKING, cast

from grizzly.context import GrizzlyContext
from grizzly.steps import step_task_write_file, step_task_write_temp_file
from grizzly.tasks import WriteFileTask

if TYPE_CHECKING:  # pragma: no cover
    from tests.fixtures import BehaveFixture


def test_step_task_write_file(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast(GrizzlyContext, behave.grizzly)
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))

    grizzly.scenario.tasks.clear()

    step_task_write_file(behave, '{{ hello }}', 'output/output.txt')

    assert len(grizzly.scenario.tasks()) == 1

    task = grizzly.scenario.tasks()[-1]

    assert isinstance(task, WriteFileTask)
    assert task.file_name == 'output/output.txt'
    assert task.content == '{{ hello }}'
    assert not task.temp_file


def test_step_task_write_temp_file(behave_fixture: BehaveFixture) -> None:
    try:
        environ['TEST_ENV'] = b64encode(b'foobar').decode()
        behave = behave_fixture.context
        grizzly = cast(GrizzlyContext, behave.grizzly)
        grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))

        grizzly.scenario.tasks.clear()

        step_task_write_temp_file(behave, '$env::TEST_ENV$', 'output/output.txt')

        assert len(grizzly.scenario.tasks()) == 1

        task = grizzly.scenario.tasks()[-1]

        assert isinstance(task, WriteFileTask)
        assert task.file_name == 'output/output.txt'
        assert task.content == 'foobar'
        assert task.temp_file
    finally:
        with suppress(Exception):
            del environ['TEST_ENV']
