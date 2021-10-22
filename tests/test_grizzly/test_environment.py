from os import environ
from typing import Any, Tuple, Dict, Optional, cast
from time import monotonic as time_monotonic

import pytest

from pytest_mock import mocker  # pylint: disable=unused-import
from pytest_mock.plugin import MockerFixture

from behave.runner import Context, Runner
from behave.configuration import Configuration
from behave.model import Feature, Step

from grizzly.environment import before_feature, after_feature, before_scenario, after_scenario, before_step, after_step
from grizzly.context import GrizzlyContext
from grizzly.steps.setup import step_setup_variable_value_ask as step_both
from grizzly.steps.background.setup import step_setup_save_statistics as step_background
from grizzly.steps.scenario.setup import step_setup_iterations as step_scenario

from .fixtures import behave_context  # pylint: disable=unused-import


def test_before_feature() -> None:
    try:
        del environ['GRIZZLY_CONTEXT_ROOT']
    except:
        pass

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

    assert not hasattr(context ,'grizzly')
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


@pytest.mark.usefixtures('behave_context')
def test_after_feature(behave_context: Context, mocker: MockerFixture) -> None:
    feature = Feature(None, None, '', '', scenarios=[behave_context.scenario])
    behave_context.scenario.steps = [Step(None, None, '', '', '')]

    class LocustRunning(Exception):
        pass

    def locustrun_running(context: Context) -> None:
        raise LocustRunning()

    mocker.patch(
        'grizzly.environment.locustrun',
        locustrun_running,
    )

    # do not start locust if feature failed
    feature.set_status('failed')

    after_feature(behave_context, feature)

    # start locust only if it's not a dry run and the feature passed
    feature.set_status('passed')

    with pytest.raises(LocustRunning):
        after_feature(behave_context, feature)


    def locustrun_return_not_0(context: Context) -> int:
        return 1

    mocker.patch(
        'grizzly.environment.locustrun',
        locustrun_return_not_0,
    )

    assert feature.status == 'passed'

    after_feature(behave_context, feature)

    assert feature.status == 'failed'

    assert feature.duration == 0.0
    behave_context.started = time_monotonic() - 1.0

    after_feature(behave_context, feature)

    assert feature.duration > 0.0


@pytest.mark.usefixtures('behave_context')
def test_before_scenario(behave_context: Context, mocker: MockerFixture) -> None:

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
        'tests.test_grizzly.fixtures.step_registry.find_match',
        find_match,
    )


    background_scenario_step = Step(filename=None, line=None, keyword='', step_type='step', name='')
    background_background_step = Step(filename=None, line=None, keyword='', step_type='step', name='background')
    scenario_background_step = Step(filename=None, line=None, keyword='', step_type='step', name='background')
    both_step = Step(filename=None, line=None, keyword='', step_type='step', name='both')
    local_step = Step(filename=None, line=None, keyword='', step_type='step', name='local')

    behave_context.scenario.name = 'Test Scenario'
    behave_context.scenario.background.steps = [
        background_scenario_step,
        background_background_step,
        both_step,
        local_step,
        None,
    ]

    behave_context.scenario.steps += [scenario_background_step, both_step, local_step, None]

    assert len(behave_context.scenario.steps) == 5
    assert len(behave_context.scenario.background.steps) == 5

    grizzly = cast(GrizzlyContext, behave_context.grizzly)

    assert len(grizzly.scenarios()) == 0

    before_scenario(behave_context, behave_context.scenario)

    assert len(grizzly.scenarios()) == 1
    assert grizzly.scenarios()[0] is grizzly.scenario
    assert grizzly.scenario.name == 'Test Scenario'
    assert getattr(behave_context.scenario.background.steps[0], 'location_status', None) == 'incorrect'
    assert getattr(behave_context.scenario.background.steps[1], 'location_status', None) is None
    assert getattr(behave_context.scenario.background.steps[2], 'location_status', None) is None
    assert getattr(behave_context.scenario.background.steps[3], 'location_status', None) is None
    assert getattr(behave_context.scenario.steps[0], 'location_status', None) is None
    assert getattr(behave_context.scenario.steps[1], 'location_status', None) == 'incorrect'
    assert getattr(behave_context.scenario.steps[2], 'location_status', None) is None
    assert getattr(behave_context.scenario.steps[3], 'location_status', None) is None

    grizzly.state.background_section_done = True
    grizzly._scenarios = []

    before_scenario(behave_context, behave_context.scenario)

    assert behave_context.scenario.background is None


@pytest.mark.usefixtures('behave_context')
def test_after_scenario(behave_context: Context) -> None:
    grizzly = cast(GrizzlyContext, behave_context.grizzly)

    assert not grizzly.state.background_section_done

    after_scenario(behave_context)

    assert grizzly.state.background_section_done

    after_scenario(behave_context)

    assert grizzly.state.background_section_done


@pytest.mark.usefixtures('behave_context')
def test_before_step(behave_context: Context) -> None:
    step = Step(filename=None, line=None, keyword='', step_type='step', name='')
    behave_context.step = None

    before_step(behave_context, step)

    assert behave_context.step is step

    setattr(step, 'location_status', 'incorrect')

    with pytest.raises(AssertionError):
        before_step(behave_context, step)

    setattr(step, 'location_status', 'incorrect')

    with pytest.raises(AssertionError):
        before_step(behave_context, step)


@pytest.mark.usefixtures('behave_context')
def test_after_step(behave_context: Context) -> None:
    step = Step(filename=None, line=None, keyword='', step_type='step', name='')
    after_step(behave_context, step)
