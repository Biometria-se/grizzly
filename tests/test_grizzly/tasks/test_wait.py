from typing import Any, Tuple, Dict

from pytest_mock import MockerFixture

from grizzly.tasks import WaitTask

from ...fixtures import GrizzlyFixture


class TestWaitTask:
    def test(self, mocker: MockerFixture, grizzly_fixture: GrizzlyFixture) -> None:
        task_factory = WaitTask(time=1.0)

        assert task_factory.time == 1.0
        task = task_factory()

        assert callable(task)

        _, _, scenario = grizzly_fixture()

        assert scenario is not None

        def noop(*args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> Any:
            pass

        import grizzly.tasks.wait
        mocker.patch.object(grizzly.tasks.wait, 'gsleep', noop)
        gsleep_spy = mocker.spy(grizzly.tasks.wait, 'gsleep')

        task(scenario)

        assert gsleep_spy.call_count == 1
        args, _ = gsleep_spy.call_args_list[0]
        assert args[0] == task_factory.time
