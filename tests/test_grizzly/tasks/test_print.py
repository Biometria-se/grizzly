import logging

from typing import Callable

import pytest

from _pytest.logging import LogCaptureFixture

from grizzly.tasks import PrintTask

from ..fixtures import grizzly_context, request_task, locust_environment  # pylint: disable=unused-import

class TestPrintTask:
    @pytest.mark.usefixtures('grizzly_context')
    def test(self, grizzly_context: Callable, caplog: LogCaptureFixture) -> None:
        task = PrintTask(message='hello world!')
        assert task.message == 'hello world!'

        implementation = task.implementation()

        assert callable(implementation)

        _, _, tasks, _ = grizzly_context()

        with caplog.at_level(logging.INFO):
            implementation(tasks)
        assert 'hello world!' in caplog.text
        caplog.clear()

        task = PrintTask(message='variable={{ variable }}')
        assert task.message == 'variable={{ variable }}'

        implementation = task.implementation()

        assert callable(implementation)

        tasks.user._context['variables']['variable'] = 'hello world!'

        with caplog.at_level(logging.INFO):
            implementation(tasks)
        assert 'variable=hello world!' in caplog.text
        caplog.clear()
