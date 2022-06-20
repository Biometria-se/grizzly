import logging

from _pytest.logging import LogCaptureFixture

from grizzly.tasks import LogMessageTask

from ...fixtures import GrizzlyFixture


class TestLogMessageTask:
    def test(self, grizzly_fixture: GrizzlyFixture, caplog: LogCaptureFixture) -> None:
        task_factory = LogMessageTask(message='hello world!')
        assert task_factory.message == 'hello world!'

        task = task_factory()

        assert callable(task)

        _, _, scenario = grizzly_fixture()

        assert scenario is not None

        with caplog.at_level(logging.INFO):
            task(scenario)
        assert 'hello world!' in caplog.text
        caplog.clear()

        task_factory = LogMessageTask(message='variable={{ variable }}')
        assert task_factory.message == 'variable={{ variable }}'

        task = task_factory()

        assert callable(task)

        scenario.user._context['variables']['variable'] = 'hello world!'

        with caplog.at_level(logging.INFO):
            task(scenario)
        assert 'variable=hello world!' in caplog.text
        caplog.clear()
