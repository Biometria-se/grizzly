import logging

from _pytest.logging import LogCaptureFixture

from grizzly.tasks import LogMessageTask

from tests.fixtures import GrizzlyFixture


class TestLogMessageTask:
    def test(self, grizzly_fixture: GrizzlyFixture, caplog: LogCaptureFixture) -> None:
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

        parent.user._context['variables']['variable'] = 'hello world!'

        with caplog.at_level(logging.INFO):
            task(parent)
        assert 'variable=hello world!' in caplog.text
        caplog.clear()
