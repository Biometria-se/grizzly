import logging

from os import environ
from typing import Any, Dict, Tuple, List, Optional, cast
from time import perf_counter as time
from datetime import datetime
from pathlib import Path
from json import loads as jsonloads

import setproctitle as proc

from grizzly.types import RequestType
from grizzly.types.behave import Context, Feature, Step, Scenario, Status

from .context import GrizzlyContext
from .testdata.variables import destroy_variables
from .locust import run as locustrun, on_worker
from .utils import check_mq_client_logs, fail_direct, in_correct_section

logger = logging.getLogger(__name__)

try:
    import pymqi  # pylint: disable=unused-import
except ModuleNotFoundError:
    from grizzly_extras import dummy_pymqi as pymqi


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
    context.last_task_count = {}


def after_feature(context: Context, feature: Feature, *args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> None:
    return_code: int
    cause: str

    # all scenarios has been processed, let's run locust
    if feature.status == Status.passed:
        status: Optional[Status] = None

        return_code = locustrun(context)

        if return_code != 0:
            status = Status.failed if return_code != 15 else Status.skipped
            feature.set_status(status)

        if not on_worker(context):
            grizzly = cast(GrizzlyContext, context.grizzly)
            stats = grizzly.state.locust.environment.stats

            if status is None:
                status = Status.failed

            for behave_scenario in cast(List[Scenario], feature.scenarios):
                grizzly_scenario = grizzly.scenarios.find_by_description(behave_scenario.name)
                if grizzly_scenario is None:
                    continue

                total_errors = 0
                for error in stats.errors.values():
                    if error.name.startswith(grizzly_scenario.identifier):
                        total_errors += 1

                scenario_stat = stats.get(grizzly_scenario.locust_name, RequestType.SCENARIO())

                if scenario_stat.num_failures > 0 or scenario_stat.num_requests != grizzly_scenario.iterations or total_errors > 0:
                    behave_scenario.set_status(status)
                    if return_code == 0:
                        return_code = 1

        if pymqi.__name__ != 'grizzly_extras.dummy_pymqi' and not on_worker(context):
            check_mq_client_logs(context)

        if return_code != 0:
            cause = 'locust test failed' if return_code != 15 else 'locust test aborted'
    else:
        cause = 'failed to prepare locust test'
        return_code = 1

    if not on_worker(context):
        # show start and stop date time
        stopped = datetime.now().astimezone()

        end_text = 'Aborted' if return_code == 15 else 'Finished'

        print('', flush=True)
        print(f'{"Started":<{len(end_text)}}: {context.started}')
        print(f'{end_text}: {stopped}')

        if return_code != 0:
            print('')

        # the features duration is the sum of all scenarios duration, which is the sum of all steps duration
        try:
            duration = int(time() - context.start)

            feature.scenarios[-1].steps[-1].duration = duration
        except Exception:
            pass

        if return_code != 0:
            raise RuntimeError(cause)


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

    if grizzly.scenario.identifier not in context.last_task_count:
        context.last_task_count[grizzly.scenario.identifier] = 0


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
        last_task_index = context.last_task_count.get(grizzly.scenario.identifier, None)

        if last_task_index is not None and task_index > last_task_index:
            context.last_task_count[grizzly.scenario.identifier] = task_index
            # GrizzlyIterator offset by one, since it has an interal task which is not represented by a step
            grizzly.scenario.tasks.behave_steps.update({task_index + 1: f'{step.keyword} {step.name}'})
