from os import environ
from typing import Any, Dict, Tuple, List, cast
from time import perf_counter as time
from datetime import datetime
from pathlib import Path
from json import loads as jsonloads

import setproctitle as proc

from behave.runner import Context
from behave.model import Feature, Step, Scenario
from behave.model_core import Status

from .context import GrizzlyContext
from .testdata.variables import destroy_variables
from .locust import run as locustrun, on_worker
from .utils import catch, check_mq_client_logs, fail_direct, in_correct_section
from .types import RequestType

try:
    import pymqi  # pylint: disable=unused-import
except ModuleNotFoundError:
    from grizzly_extras import dummy_pymqi as pymqi


last_task_count: Dict[str, int] = {}


def before_feature(context: Context, feature: Feature, *args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> None:
    # identify as grizzly, instead of behave
    proc.setproctitle('grizzly')

    environ['GRIZZLY_CONTEXT_ROOT'] = context.config.base_dir
    environ['GRIZZLY_FEATURE_FILE'] = feature.filename

    destroy_variables()

    try:
        GrizzlyContext.destroy()
    except ValueError:
        pass

    grizzly = GrizzlyContext()
    grizzly.state.verbose = context.config.verbose

    persistent_file = Path(context.config.base_dir) / 'persistent' / f'{Path(feature.filename).stem}.json'

    if persistent_file.exists():
        grizzly.state.persistent = jsonloads(persistent_file.read_text())

    context.grizzly = grizzly
    context.start = time()
    context.started = datetime.now().astimezone()


@catch(KeyboardInterrupt)
def after_feature(context: Context, feature: Feature, *args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> None:
    # all scenarios has been processed, let's run locust
    if feature.status == Status.passed:
        return_code = locustrun(context)

        if return_code != 0:
            feature.set_status(Status.failed)

        grizzly = cast(GrizzlyContext, context.grizzly)
        stats = grizzly.state.locust.environment.stats

        for behave_scenario in cast(List[Scenario], feature.scenarios):
            grizzly_scenario = grizzly.scenarios.find_by_description(behave_scenario.name)
            if grizzly_scenario is None:
                continue

            scenario_stat = stats.get(grizzly_scenario.locust_name, RequestType.SCENARIO())

            if scenario_stat.num_failures > 0 or scenario_stat.num_requests != grizzly_scenario.iterations:
                behave_scenario.set_status(Status.failed)

            rindex = -1

            for stat in stats.entries.values():
                if stat.method == RequestType.SCENARIO() or not stat.name.startswith(f'{grizzly_scenario.identifier} '):
                    continue

                if stat.num_failures > 0:
                    rindex -= 1
                    behave_step = cast(Step, behave_scenario.steps[rindex])
                    behave_step.status = Status.failed

        if pymqi.__name__ != 'grizzly_extras.dummy_pymqi' and not on_worker(context):
            check_mq_client_logs(context)
    else:
        return_code = 1

    # show start and stop date time
    stopped = datetime.now().astimezone()

    print('')
    print(f'Started: {context.started}')
    print(f'Stopped: {stopped}')

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

    grizzly.scenarios.create(scenario)

    if grizzly.scenario.identifier not in last_task_count:
        last_task_count[grizzly.scenario.identifier] = 0


def after_scenario(context: Context, *args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> None:
    grizzly = cast(GrizzlyContext, context.grizzly)

    # first scenario is done, do not process background for any (possible) other scenarios
    if not grizzly.state.background_section_done:
        grizzly.state.background_section_done = True

    assert grizzly.scenario.tasks.tmp.async_group is None, f'async request group "{grizzly.scenario.tasks.tmp.async_group.name}" has not been closed'
    assert grizzly.scenario.tasks.tmp.loop is None, f'loop task "{grizzly.scenario.tasks.tmp.loop.name}" has not been closed'
    open_timers = {name: timer for name, timer in grizzly.scenario.tasks.tmp.timers.items() if timer is not None}
    assert len(open_timers) < 1, f'timers {", ".join(open_timers.keys())} has not been closed'
    assert grizzly.scenario.tasks.tmp.conditional is None, f'conditional "{grizzly.scenario.tasks.tmp.conditional.name}" has not been closed'


def before_step(context: Context, step: Step, *args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> None:
    # fail step if it's a @backgroundsection decorated step implementation, see before_scenario hook
    with fail_direct(context):
        assert not getattr(step, 'location_status', '') == 'incorrect', 'Step is in the incorrect section'

    # add current step to context, used else where
    context.step = step


def after_step(context: Context, step: Step, *args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> None:
    # grizzly does not have any functionality that should run after every step, but added for
    # clarity of what can be overloaded
    grizzly = cast(GrizzlyContext, context.grizzly)

    if len(grizzly.scenario.tasks._tmp.__stack__) == 0:
        task_index = len(grizzly.scenario.tasks)

        if task_index > last_task_count[grizzly.scenario.identifier]:
            last_task_count[grizzly.scenario.identifier] = task_index
            # GrizzlyIterator offset by one, since it has an interal task
            grizzly.scenario.tasks.behave_steps.update({task_index + 1: f'{step.keyword} {step.name}'})
