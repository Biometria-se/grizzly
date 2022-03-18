import logging

from _pytest.logging import LogCaptureFixture

from grizzly.tasks import PrintTask

from ..fixtures import GrizzlyFixture


class TestPrintTask:
    def test(self, grizzly_fixture: GrizzlyFixture, caplog: LogCaptureFixture) -> None:
        task = PrintTask(message='hello world!')
        assert task.message == 'hello world!'

        implementation = task.implementation()

        assert callable(implementation)

        _, _, scenario = grizzly_fixture()

        assert scenario is not None

        with caplog.at_level(logging.INFO):
            implementation(scenario)
        assert 'hello world!' in caplog.text
        caplog.clear()

        task = PrintTask(message='variable={{ variable }}')
        assert task.message == 'variable={{ variable }}'

        implementation = task.implementation()

        assert callable(implementation)

        scenario.user._context['variables']['variable'] = 'hello world!'

        with caplog.at_level(logging.INFO):
            implementation(scenario)
        assert 'variable=hello world!' in caplog.text
        caplog.clear()
