import subprocess
import sys

from os import environ
from typing import Any, Tuple, Dict, Optional, cast
from time import perf_counter
from json import dumps as jsondumps
from shutil import rmtree
from datetime import datetime

import pytest

from _pytest.tmpdir import TempPathFactory
from _pytest.capture import CaptureFixture
from pytest_mock import MockerFixture
from behave.runner import Runner
from behave.configuration import Configuration

from grizzly.types.behave import Context, Feature, Step, Status
from grizzly.behave import before_feature, after_feature, before_scenario, after_scenario, before_step, after_step
from grizzly.context import GrizzlyContext
from grizzly.steps.setup import step_setup_variable_value_ask as step_both
from grizzly.steps.background.setup import step_setup_save_statistics as step_background
from grizzly.steps.scenario.setup import step_setup_iterations as step_scenario
from grizzly.tasks import AsyncRequestGroupTask, TimerTask, ConditionalTask, LoopTask, LogMessageTask
from grizzly.types import RequestType

from tests.fixtures import BehaveFixture, GrizzlyFixture
from tests.helpers import onerror


def test_behave_no_pymqi_dependencies() -> None:
    env = environ.copy()
    try:
        del env['LD_LIBRARY_PATH']
    except KeyError:
        pass

    env['PYTHONPATH'] = '.'

    process = subprocess.Popen(
        [
            sys.executable,
            '-c',
            (
                'import grizzly.behave as gbehave;'
                'print(f"{gbehave.pymqi.__name__=}");'
            ),
        ],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )

    out, _ = process.communicate()
    output = out.decode()

    assert process.returncode == 0
    assert "gbehave.pymqi.__name__='grizzly_extras.dummy_pymqi'" in output


def test_before_feature(behave_fixture: BehaveFixture, tmp_path_factory: TempPathFactory) -> None:
    context_root = tmp_path_factory.mktemp('test-context')

    behave = behave_fixture.context
    for key in ['GRIZZLY_CONTEXT_ROOT', 'GRIZZLY_FEATURE_FILE']:
        try:
            del environ[key]
        except:
            pass

    try:
        context = Context(
            runner=Runner(
                config=Configuration(
                    command_args=[],
                    load_config=False,
                    base_dir=str(context_root),
                )
            )
        )
        feature = Feature('test.feature', None, '', '', scenarios=[behave.scenario])

        assert not hasattr(context, 'grizzly')
        assert environ.get('GRIZZLY_CONTEXT_ROOT', 'notset') == 'notset'
        assert environ.get('GRIZZLY_FEATURE_FILE', 'notset') == 'notset'

        before_feature(context, feature)

        assert hasattr(context, 'grizzly')
        grizzly = cast(GrizzlyContext, context.grizzly)
        assert environ.get('GRIZZLY_CONTEXT_ROOT', None) == str(context_root)
        assert environ.get('GRIZZLY_FEATURE_FILE', None) == 'test.feature'
        assert grizzly.state.persistent == {}
        assert context.last_task_count == {}

        context.grizzly = None

        (context_root / 'persistent').mkdir(exist_ok=True, parents=True)
        (context_root / 'persistent' / 'test.json').write_text(jsondumps({'foo': 'bar', 'hello': 'world'}, indent=2))

        before_feature(context, feature)

        assert hasattr(context, 'started')
        assert hasattr(context, 'grizzly')
        grizzly = cast(GrizzlyContext, context.grizzly)
        assert grizzly.state.persistent == {
            'foo': 'bar',
            'hello': 'world',
        }

        GrizzlyContext.destroy()

        before_feature(context, feature)

        assert hasattr(context, 'started')
        assert hasattr(context, 'grizzly')
        grizzly = cast(GrizzlyContext, context.grizzly)
        assert grizzly.state.persistent == {
            'foo': 'bar',
            'hello': 'world',
        }
    finally:
        for key in ['GRIZZLY_CONTEXT_ROOT', 'GRIZZLY_FEATURE_FILE']:
            try:
                del environ[key]
            except:
                pass

        rmtree(context_root, onerror=onerror)


def test_after_feature(grizzly_fixture: GrizzlyFixture, mocker: MockerFixture, capsys: CaptureFixture) -> None:
    behave = grizzly_fixture.behave.context
    grizzly = grizzly_fixture.grizzly
    feature = Feature(None, None, '', '', scenarios=[behave.scenario])
    behave.scenario.steps = [Step(None, None, '', '', ''), Step(None, None, '', '', '')]
    behave.started = datetime.now().astimezone()

    locustrun_mock = mocker.patch(
        'grizzly.behave.locustrun',
        return_value=0
    )

    # do not start locust if feature failed
    feature.set_status(Status.failed)

    with pytest.raises(RuntimeError) as re:
        after_feature(behave, feature)
    assert str(re.value) == 'failed to prepare locust test'

    locustrun_mock.assert_not_called()

    # start locust only if it's not a dry run and the feature passed
    feature.set_status(Status.passed)

    after_feature(behave, feature)

    assert feature.status == Status.passed
    locustrun_mock.assert_called_once_with(behave)
    locustrun_mock.reset_mock()

    capsys.readouterr()

    # locustrun failed, and we actually had a grizzly scenario matching the behave scenario
    locustrun_mock.return_value = 1
    behave.scenario.name = 'test'
    grizzly.scenarios.clear()
    grizzly.scenarios.create(behave.scenario)
    grizzly.scenario.description = behave.scenario.name

    with pytest.raises(RuntimeError) as re:
        after_feature(behave, feature)
    assert str(re.value) == 'locust test failed'

    locustrun_mock.assert_called_once_with(behave)
    locustrun_mock.reset_mock()

    capture = capsys.readouterr()

    assert feature.status == Status.failed
    assert feature.duration == 0.0
    assert capture.err == ''

    # fail based on locust statistics having a feature/scenario that has failed
    feature.set_status(Status.passed)
    locustrun_mock.return_value = 0
    grizzly.state.locust.environment.stats.log_error(RequestType.SCENARIO(), '001 test', 'error error')
    grizzly.state.locust.environment.stats.log_error(RequestType.SCENARIO(), '002 test', 'error error')

    with pytest.raises(RuntimeError) as re:
        after_feature(behave, feature)
    assert str(re.value) == 'locust test failed'

    locustrun_mock.assert_called_once_with(behave)
    locustrun_mock.reset_mock()

    # if the scenario had any errors, we should fail
    grizzly.state.locust.environment.stats.clear_all()
    grizzly.state.locust.environment.stats.log_request(RequestType.SCENARIO(), '001 test', 2, 3)
    grizzly.state.locust.environment.stats.log_error(RequestType.UNTIL(), '001 until', 'error error')
    grizzly.state.locust.environment.stats.log_error('GET', '001 get', 'foobared')

    with pytest.raises(RuntimeError) as re:
        after_feature(behave, feature)
    assert str(re.value) == 'locust test failed'

    locustrun_mock.assert_called_once_with(behave)
    locustrun_mock.reset_mock()

    behave.start = perf_counter() - 1.0
    feature.set_status(Status.passed)
    locustrun_mock.return_value = 123

    with pytest.raises(RuntimeError) as re:
        after_feature(behave, feature)
    assert str(re.value) == 'locust test failed'

    capture = capsys.readouterr()

    assert feature.status == Status.failed
    assert feature.duration > 0.0
    assert capture.err == ''


def test_before_scenario(behave_fixture: BehaveFixture, mocker: MockerFixture) -> None:
    behave = behave_fixture.context

    class MatchedStep:
        def __init__(self, name: str) -> None:
            if name == 'background':
                self.func = step_background
            elif name == 'both':
                self.func = step_both
            elif name == 'local':
                self.func = self.step_local
            else:
                self.func = step_scenario

        def step_local(self) -> None:
            pass

    def find_match(step: Step, *args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> Optional[MatchedStep]:
        if step is None:
            return None

        return MatchedStep(step.name)

    mocker.patch(
        'tests.fixtures.step_registry.find_match',
        find_match,
    )

    background_scenario_step = Step(filename=None, line=None, keyword='', step_type='step', name='')
    background_background_step = Step(filename=None, line=None, keyword='', step_type='step', name='background')
    scenario_background_step = Step(filename=None, line=None, keyword='', step_type='step', name='background')
    both_step = Step(filename=None, line=None, keyword='', step_type='step', name='both')
    local_step = Step(filename=None, line=None, keyword='', step_type='step', name='local')

    behave.scenario.name = 'Test Scenario'
    behave.scenario.background.steps = [
        background_scenario_step,
        background_background_step,
        both_step,
        local_step,
        None,
    ]

    behave.scenario.steps += [scenario_background_step, both_step, local_step, None]
    behave.last_task_count = {}

    assert len(behave.scenario.steps) == 5
    assert len(behave.scenario.background.steps) == 5

    grizzly = cast(GrizzlyContext, behave.grizzly)

    assert len(grizzly.scenarios()) == 0

    before_scenario(behave, behave.scenario)

    assert len(grizzly.scenarios()) == 1
    assert grizzly.scenarios()[0] is grizzly.scenario
    assert grizzly.scenario.name == 'Test Scenario'
    assert getattr(behave.scenario.background.steps[0], 'location_status', None) == 'incorrect'
    assert getattr(behave.scenario.background.steps[1], 'location_status', None) is None
    assert getattr(behave.scenario.background.steps[2], 'location_status', None) is None
    assert getattr(behave.scenario.background.steps[3], 'location_status', None) is None
    assert getattr(behave.scenario.steps[0], 'location_status', None) is None
    assert getattr(behave.scenario.steps[1], 'location_status', None) == 'incorrect'
    assert getattr(behave.scenario.steps[2], 'location_status', None) is None
    assert getattr(behave.scenario.steps[3], 'location_status', None) is None

    grizzly.state.background_section_done = True
    grizzly.scenarios.clear()

    before_scenario(behave, behave.scenario)

    assert behave.scenario.background is None


def test_after_scenario(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = behave_fixture.grizzly
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))
    grizzly.scenario.tasks.tmp.async_group = AsyncRequestGroupTask(name='test-async-1')

    with pytest.raises(AssertionError) as ae:
        after_scenario(behave)
    assert str(ae.value) == 'async request group "test-async-1" has not been closed'

    grizzly.scenario.tasks.tmp.async_group = None

    grizzly.scenario.tasks.tmp.timers['test-timer-1'] = TimerTask('test-timer-1')
    grizzly.scenario.tasks.tmp.timers['test-timer-2'] = TimerTask('test-timer-2')

    with pytest.raises(AssertionError) as ae:
        after_scenario(behave)
    assert str(ae.value) == 'timers test-timer-1, test-timer-2 has not been closed'

    grizzly.scenario.tasks.tmp.timers.clear()

    grizzly.scenario.tasks.tmp.conditional = ConditionalTask(
        name='test-conditional-1',
        condition='{{ value | int > 0 }}',
    )

    with pytest.raises(AssertionError) as ae:
        after_scenario(behave)
    assert str(ae.value) == 'conditional "test-conditional-1" has not been closed'

    grizzly.scenario.tasks.tmp.conditional = None
    grizzly.state.background_section_done = False
    grizzly.state.variables['foobar'] = 'none'
    grizzly.scenario.tasks.tmp.loop = LoopTask(grizzly, name='test-loop', values='["hello", "world"]', variable='foobar')

    with pytest.raises(AssertionError) as ae:
        after_scenario(behave)
    assert str(ae.value) == 'loop task "test-loop" has not been closed'

    grizzly.scenario.tasks.tmp.loop = None
    grizzly.state.background_section_done = False

    assert not grizzly.state.background_section_done

    after_scenario(behave)

    assert getattr(grizzly.state, 'background_section_done', False)

    after_scenario(behave)

    assert grizzly.state.background_section_done


def test_before_step(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    step = Step(filename=None, line=None, keyword='', step_type='step', name='')
    behave.step = None

    before_step(behave, step)

    assert behave.step is step

    setattr(step, 'location_status', 'incorrect')

    with pytest.raises(AssertionError):
        before_step(behave, step)

    setattr(step, 'location_status', 'incorrect')

    with pytest.raises(AssertionError):
        before_step(behave, step)


def test_after_step(grizzly_fixture: GrizzlyFixture) -> None:
    behave = grizzly_fixture.behave.context
    grizzly = grizzly_fixture.grizzly

    behave.last_task_count = {}
    step = Step(filename=None, line=None, keyword='And', step_type='step', name='this is a grizzly step')

    after_step(behave, step)

    assert grizzly.scenario.tasks.behave_steps == {}

    # create grizzly scenario
    behave.feature = Feature(None, None, '', '', scenarios=[behave.scenario])
    behave.scenario.name = 'test'
    behave.scenario.steps = [step]
    grizzly.scenarios.create(behave.scenario)
    grizzly.scenario.tasks.add(LogMessageTask('hello world'))
    behave.last_task_count = {grizzly.scenario.identifier: 0}

    after_step(behave, step)

    assert grizzly.scenario.tasks.behave_steps == {2: 'And this is a grizzly step'}
