from os import environ
from typing import Any, Tuple, Dict, Optional, cast
from time import monotonic as time_monotonic

import pytest

from pytest_mock import MockerFixture
from behave.runner import Context, Runner
from behave.configuration import Configuration
from behave.model import Feature, Step, Status

from grizzly.environment import before_feature, after_feature, before_scenario, after_scenario, before_step, after_step
from grizzly.context import GrizzlyContext
from grizzly.steps.setup import step_setup_variable_value_ask as step_both
from grizzly.steps.background.setup import step_setup_save_statistics as step_background
from grizzly.steps.scenario.setup import step_setup_iterations as step_scenario
from grizzly.tasks import AsyncRequestGroupTask, TimerTask, ConditionalTask

from ..fixtures import BehaveFixture


def test_before_feature() -> None:
    try:
        del environ['GRIZZLY_CONTEXT_ROOT']
    except:
        pass

    try:
        base_dir = '.'
        context = Context(
            runner=Runner(
                config=Configuration(
                    command_args=[],
                    load_config=False,
                    base_dir=base_dir,
                )
            )
        )

        assert not hasattr(context, 'grizzly')
        assert environ.get('GRIZZLY_CONTEXT_ROOT', None) is None

        before_feature(context)

        assert hasattr(context, 'grizzly')
        assert context.grizzly.__class__.__name__ == 'GrizzlyContext'
        assert environ.get('GRIZZLY_CONTEXT_ROOT', None) == base_dir

        context.grizzly = object()

        before_feature(context)

        assert hasattr(context, 'grizzly')
        assert context.grizzly.__class__.__name__ == 'GrizzlyContext'

        assert hasattr(context, 'started')
    finally:
        try:
            del environ['GRIZZLY_CONTEXT_ROOT']
        except:
            pass


def test_after_feature(behave_fixture: BehaveFixture, mocker: MockerFixture) -> None:
    behave = behave_fixture.context
    feature = Feature(None, None, '', '', scenarios=[behave.scenario])
    behave.scenario.steps = [Step(None, None, '', '', '')]

    class LocustRunning(Exception):
        pass

    def locustrun_running(context: Context) -> None:
        raise LocustRunning()

    mocker.patch(
        'grizzly.environment.locustrun',
        locustrun_running,
    )

    # do not start locust if feature failed
    feature.set_status(Status.failed)

    after_feature(behave, feature)

    # start locust only if it's not a dry run and the feature passed
    feature.set_status(Status.passed)

    with pytest.raises(LocustRunning):
        after_feature(behave, feature)

    def locustrun_return_not_0(context: Context) -> int:
        return 1

    mocker.patch(
        'grizzly.environment.locustrun',
        locustrun_return_not_0,
    )

    assert feature.status == Status.passed

    after_feature(behave, feature)

    assert feature.status == Status.failed

    assert feature.duration == 0.0
    behave.start = time_monotonic() - 1.0

    after_feature(behave, feature)

    assert feature.duration > 0.0


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


def test_after_step(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    step = Step(filename=None, line=None, keyword='', step_type='step', name='')
    after_step(behave, step)
