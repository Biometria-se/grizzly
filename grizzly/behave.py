"""Grizzly hooks into behave."""
from __future__ import annotations

import logging
from contextlib import suppress
from datetime import datetime
from json import loads as jsonloads
from os import environ
from pathlib import Path
from textwrap import indent
from time import perf_counter as time
from typing import Any, Dict, List, Optional, cast

import setproctitle as proc
from behave.reporter.summary import SummaryReporter

from .context import GrizzlyContext
from .exceptions import FeatureError, StepError
from .locust import on_worker
from .locust import run as locustrun
from .testdata.variables import destroy_variables
from .types import RequestType
from .types.behave import Context, Feature, Scenario, Status, Step
from .utils import check_mq_client_logs, fail_direct, in_correct_section

logger = logging.getLogger(__name__)

try:
    import pymqi
except ModuleNotFoundError:
    from grizzly_extras import dummy_pymqi as pymqi


ABORTED_RETURN_CODE = 15
IN_CORRECT_SECTION_ATTRIBUTE = 'location_status'


def before_feature(context: Context, feature: Feature, *_args: Any, **_kwargs: Any) -> None:
    # identify as grizzly, instead of behave
    proc.setproctitle('grizzly')

    environ['GRIZZLY_CONTEXT_ROOT'] = context.config.base_dir
    environ['GRIZZLY_FEATURE_FILE'] = feature.filename

    destroy_variables()

    with suppress(ValueError):
        GrizzlyContext.destroy()

    grizzly = GrizzlyContext()
    grizzly.state.verbose = context.config.verbose

    persistent_file = Path(context.config.base_dir) / 'persistent' / f'{Path(feature.filename).stem}.json'

    if persistent_file.exists():
        grizzly.state.persistent = jsonloads(persistent_file.read_text())

    context.grizzly = grizzly
    context.start = time()
    context.started = datetime.now().astimezone()
    context.last_task_count = {}
    context.exceptions = {}


def after_feature_master(return_code: int, status: Optional[Status], context: Context, feature: Feature) -> int:
    """Master should validate locust request statistics to determine if feature was successful or not."""
    if on_worker(context):
        return return_code

    grizzly = cast(GrizzlyContext, context.grizzly)

    if not hasattr(grizzly.state, 'locust'):
        return 0

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

    return return_code


def after_feature(context: Context, feature: Feature, *_args: Any, **_kwargs: Any) -> None:  # noqa: PLR0912
    return_code: int
    cause: str
    has_exceptions = hasattr(context, 'exceptions') and len(context.exceptions) > 0

    reporter: SummaryReporter

    for possible_reporter in context.config.reporters:
        if isinstance(possible_reporter, SummaryReporter):
            reporter = possible_reporter
            break

    # all scenarios has been processed, let's run locust
    if feature.status == Status.passed and not has_exceptions:
        status: Optional[Status] = None

        try:
            return_code = locustrun(context)
        except Exception as e:
            has_exceptions = True
            context.exceptions.update({None: [*context.exceptions.get(None, []), FeatureError(e)]})
            return_code = 1

        if return_code != 0:
            status = Status.failed if return_code != ABORTED_RETURN_CODE else Status.skipped
            feature.set_status(status)

        # optional checks, that should not be executed when it's a dry run
        if environ.get('GRIZZLY_DRY_RUN', 'false').lower() != 'true':
            return_code = after_feature_master(return_code, status, context, feature)

            if pymqi.__name__ != 'grizzly_extras.dummy_pymqi' and not on_worker(context):
                check_mq_client_logs(context)

        if return_code != 0:
            cause = 'locust test failed' if return_code != ABORTED_RETURN_CODE else 'locust test aborted'
    else:
        return_code = 1
        cause = 'failed to prepare locust test'

    if on_worker(context):
        return

    # show start and stop date time
    stopped = datetime.now().astimezone()

    reporter.stream.flush()

    if has_exceptions:
        buffer: list[str] = []

        for scenario_name, exceptions in cast(Dict[Optional[str], List[AssertionError]], context.exceptions).items():
            if scenario_name is not None:
                buffer.append(f'Scenario: {scenario_name}')
            else:
                buffer.append('')
            buffer.extend([indent(str(exception), '    ') for exception in exceptions])
            buffer.append('')

        failure_summary = indent('\n'.join(buffer), '    ')
        reporter.stream.write(f'\nFailure summary:\n{failure_summary}')

    end_text = 'Aborted' if return_code == ABORTED_RETURN_CODE else 'Finished'

    reporter.stream.write(f'\n{"Started":<{len(end_text)}}: {context.started}\n{end_text}: {stopped}\n\n')

    # the features duration is the sum of all scenarios duration, which is the sum of all steps duration
    with suppress(Exception):
        duration = int(time() - context.start)
        feature.scenarios[-1].steps[-1].duration = duration

    if return_code != 0:
        raise RuntimeError(cause)


def before_scenario(context: Context, scenario: Scenario, *_args: Any, **_kwargs: Any) -> None:
    grizzly = cast(GrizzlyContext, context.grizzly)

    if grizzly.state.background_section_done:
        scenario.background = None
    else:
        steps = scenario.background.steps if scenario.background is not None else []
        for step in steps:
            matched_step = context._runner.step_registry.find_match(step)

            # unimplemented step, let behave handle it later on
            if matched_step is None:
                continue

            if not in_correct_section(matched_step.func, ['grizzly.steps.background', 'grizzly.steps']):
                setattr(step, IN_CORRECT_SECTION_ATTRIBUTE, 'incorrect')

    # check that a @backgroundsection decorated step implementation isn't in a Scenario section
    for step in scenario.steps:
        matched_step = context._runner.step_registry.find_match(step)

        # unimplemented step, let behave handle it later on
        if matched_step is None:
            continue

        if not in_correct_section(matched_step.func, ['grizzly.steps.scenario', 'grizzly.steps.scenario.tasks', 'grizzly.steps']):
            # to get a nicer error message, the step should fail before it's executed, see before_step hook
            setattr(step, IN_CORRECT_SECTION_ATTRIBUTE, 'incorrect')

    grizzly.scenarios.create(scenario)

    if grizzly.scenario.identifier not in context.last_task_count:
        context.last_task_count[grizzly.scenario.identifier] = 0


def after_scenario(context: Context, *_args: Any, **_kwargs: Any) -> None:
    grizzly = cast(GrizzlyContext, context.grizzly)

    # first scenario is done, do not process background for any (possible) other scenarios
    if not grizzly.state.background_section_done:
        grizzly.state.background_section_done = True

    assert grizzly.scenario.tasks.tmp.async_group is None, f'async request group "{grizzly.scenario.tasks.tmp.async_group.name}" has not been closed'
    assert grizzly.scenario.tasks.tmp.loop is None, f'loop task "{grizzly.scenario.tasks.tmp.loop.name}" has not been closed'

    open_timers = {name: timer for name, timer in grizzly.scenario.tasks.tmp.timers.items() if timer is not None}
    assert not len(open_timers) > 0, f'timers {", ".join(open_timers.keys())} has not been closed'

    assert grizzly.scenario.tasks.tmp.conditional is None, f'conditional "{grizzly.scenario.tasks.tmp.conditional.name}" has not been closed'

    for scenario_name, exceptions in cast(Dict[Optional[str], List[StepError]], context.exceptions).items():
        if scenario_name != context.scenario.name:
            continue

        for exception in exceptions:
            exception.step.status = Status.failed


def before_step(context: Context, step: Step, *_args: Any, **_kwargs: Any) -> None:
    # fail step if it's a @backgroundsection decorated step implementation, see before_scenario hook
    with fail_direct(context):
        assert getattr(step, 'location_status', '') != 'incorrect', 'Step is in the incorrect section'

    # add current step to context, used else where
    context.step = step


def after_step(context: Context, step: Step, *_args: Any, **_kwargs: Any) -> None:
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
