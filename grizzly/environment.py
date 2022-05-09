from os import environ
from typing import Any, Dict, Tuple, cast
from time import perf_counter as time
from datetime import datetime

import setproctitle as proc

from behave.runner import Context
from behave.model import Feature, Step, Scenario
from behave.model_core import Status

from .context import GrizzlyContext
from .testdata.variables import destroy_variables
from .locust import run as locustrun
from .utils import catch, fail_direct, in_correct_section


def before_feature(context: Context, *_args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> None:
    # identify as grizzly, instead of behave
    proc.setproctitle('grizzly')

    environ['GRIZZLY_CONTEXT_ROOT'] = context.config.base_dir

    destroy_variables()

    try:
        GrizzlyContext.destroy()
    except ValueError:
        pass

    grizzly = GrizzlyContext()
    grizzly.state.verbose = context.config.verbose

    context.grizzly = grizzly
    context.start = time()
    context.started = datetime.now()


@catch(KeyboardInterrupt)
def after_feature(context: Context, feature: Feature, *args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> None:
    # all scenarios has been processed, let's run locust
    if feature.status == Status.passed:
        return_code = locustrun(context)

        if return_code != 0:
            feature.set_status('failed')

    # the features duration is the sum of all scenarios duration, which is the sum of all steps duration
    try:
        duration = int(time() - context.start)

        feature.scenarios[-1].steps[-1].duration = duration
    except Exception:
        pass


def before_scenario(context: Context, scenario: Scenario, *args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> None:
    grizzly = cast(GrizzlyContext, context.grizzly)

    if grizzly.state.background_section_done:
        scenario.background = None
    else:
        for step in scenario.background.steps:
            matched_step = context._runner.step_registry.find_match(step)

            # unimplemented step, let behave handle it later on
            if matched_step is None:
                continue

            if not in_correct_section(matched_step.func, ['grizzly.steps.background', 'grizzly.steps']):
                setattr(step, 'location_status', 'incorrect')

    # check that a @backgroundsection decorated step implementation isn't in a Scenario section
    for step in scenario.steps:
        matched_step = context._runner.step_registry.find_match(step)

        # unimplemented step, let behave handle it later on
        if matched_step is None:
            continue

        if not in_correct_section(matched_step.func, ['grizzly.steps.scenario', 'grizzly.steps']):
            # to get a nicer error message, the step should fail before it's executed, see before_step hook
            setattr(step, 'location_status', 'incorrect')

    grizzly.add_scenario(scenario)


def after_scenario(context: Context, *args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> None:
    grizzly = cast(GrizzlyContext, context.grizzly)

    # first scenario is done, do not process background for any (possible) other scenarios
    if not grizzly.state.background_section_done:
        grizzly.state.background_section_done = True

    assert grizzly.scenario.async_group is None, f'async request group "{grizzly.scenario.async_group.name}" has not been closed'


def before_step(context: Context, step: Step, *args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> None:
    # fail step if it's a @backgroundsection decorated step implementation, see before_scenario hook
    with fail_direct(context):
        assert not getattr(step, 'location_status', '') == 'incorrect', 'Step is in the incorrect section'

    # add current step to context, used else where
    context.step = step


def after_step(context: Context, step: Step, *args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> None:
    # grizzly does not have any functionality that should run after every step, but added for
    # clarity of what can be overloaded
    return
