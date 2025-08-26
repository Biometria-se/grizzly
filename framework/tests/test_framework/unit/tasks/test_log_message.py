"""Unit tests of grizzly.tasks.log_message."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from grizzly.tasks import LogMessageTask

if TYPE_CHECKING:  # pragma: no cover
    from _pytest.logging import LogCaptureFixture

    from test_framework.fixtures import GrizzlyFixture


class TestLogMessageTask:
    def test_task(self, grizzly_fixture: GrizzlyFixture, caplog: LogCaptureFixture) -> None:
        task_factory = LogMessageTask(message='hello world!')
        assert task_factory.message == 'hello world!'
        assert task_factory.__template_attributes__ == {'message'}

        task = task_factory()

        assert callable(task)

        parent = grizzly_fixture()

        with caplog.at_level(logging.INFO):
            task(parent)
        assert 'hello world!' in caplog.text
        caplog.clear()

        task_factory = LogMessageTask(message='variable={{ variable }}')
        assert task_factory.message == 'variable={{ variable }}'

        task = task_factory()

        assert callable(task)

        parent.user.set_variable('variable', 'hello world!')

        with caplog.at_level(logging.INFO):
            task(parent)
        assert 'variable=hello world!' in caplog.text
        caplog.clear()
