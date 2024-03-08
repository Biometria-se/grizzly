"""Unit tests of grizzly.tasks.write_file."""
from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from grizzly.tasks import WriteFileTask
from tests.helpers import ANY

if TYPE_CHECKING:  # pragma: no cover
    from pytest_mock import MockerFixture

    from tests.fixtures import GrizzlyFixture


class TestWriteFile:
    def test_task(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:
        parent = grizzly_fixture()

        task_factory = WriteFileTask(file_name='test/output.log', content='{{ hello }}')

        assert task_factory.file_name == 'test/output.log'
        assert task_factory.content == '{{ hello }}'
        assert task_factory.__template_attributes__ == {'file_name', 'content'}

        task = task_factory()
        assert callable(task)

        parent.user.set_context_variable('hello', 'foobar')

        expected_file = Path(task_factory._context_root) / 'requests' / 'test' / 'output.log'

        assert not expected_file.exists()

        task(parent)

        linesep = '\n'

        assert expected_file.exists()
        assert expected_file.read_text() == f'foobar{linesep}'

        task(parent)

        assert expected_file.read_text() == f'foobar{linesep}foobar{linesep}'

        task_factory = WriteFileTask(file_name='test/{{ file_name }}.log', content='{{ hello }}')
        parent.user.set_context_variable('file_name', 'output')

        task = task_factory()

        task(parent)

        assert expected_file.read_text() == f'foobar{linesep}foobar{linesep}foobar{linesep}'

        mocker.patch.object(parent, 'render', side_effect=['test/output.log', RuntimeError('no no')])
        request_fire_spy = mocker.spy(parent.user.environment.events.request, 'fire')

        task(parent)

        assert expected_file.read_text() == f'foobar{linesep}foobar{linesep}foobar{linesep}'
        request_fire_spy.assert_called_once_with(
            request_type='FWRT',
            name=f'{parent.user._scenario.identifier} FileWriteTask=>test/output.log',
            response_time=0,
            response_length=0,
            context=parent.user._context,
            exception=ANY(RuntimeError, message='no no'),
        )

