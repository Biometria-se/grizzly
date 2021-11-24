from typing import Any, Tuple, Dict, Callable

import pytest

from pytest_mock import mocker, MockerFixture  # pylint: disable=unused-import

from grizzly.task import WaitTask

from ..fixtures import grizzly_context, request_task, behave_context, locust_environment  # pylint: disable=unused-import

class TestWaitTask:
    @pytest.mark.usefixtures('grizzly_context')
    def test(self, mocker: MockerFixture, grizzly_context: Callable) -> None:
        task = WaitTask(time=1.0)

        assert task.time == 1.0
        implementation = task.implementation()

        assert callable(implementation)

        _, _, tasks, _ = grizzly_context()

        def noop(*args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> Any:
            pass

        import grizzly.task.wait
        mocker.patch.object(grizzly.task.wait, 'gsleep', noop)
        gsleep_spy = mocker.spy(grizzly.task.wait, 'gsleep')

        implementation(tasks)

        assert gsleep_spy.call_count == 1
        args, _ = gsleep_spy.call_args_list[0]
        assert args[0] == task.time
