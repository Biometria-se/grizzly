from typing import Any, Tuple, Dict

from pytest_mock import MockerFixture

from grizzly.tasks import WaitTask

from ..fixtures import GrizzlyFixture


class TestWaitTask:
    def test(self, mocker: MockerFixture, grizzly_fixture: GrizzlyFixture) -> None:
        task = WaitTask(time=1.0)

        assert task.time == 1.0
        implementation = task.implementation()

        assert callable(implementation)

        _, _, scenario = grizzly_fixture()

        assert scenario is not None

        def noop(*args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> Any:
            pass

        import grizzly.tasks.wait
        mocker.patch.object(grizzly.tasks.wait, 'gsleep', noop)
        gsleep_spy = mocker.spy(grizzly.tasks.wait, 'gsleep')

        implementation(scenario)

        assert gsleep_spy.call_count == 1
        args, _ = gsleep_spy.call_args_list[0]
        assert args[0] == task.time
