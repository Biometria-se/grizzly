"""Grizzly hooks into behave."""

from __future__ import annotations

import logging
from contextlib import suppress
from cProfile import Profile
from datetime import datetime
from json import loads as jsonloads
from os import environ
from pathlib import Path
from platform import node as gethostname
from textwrap import indent
from time import perf_counter as time
from typing import TYPE_CHECKING, Any, cast

import setproctitle as proc
from behave.reporter.summary import SummaryReporter

from .context import GrizzlyContext
from .exceptions import FeatureError, StepError
from .locust import on_worker
from .locust import run as locustrun
from .testdata import filters
from .testdata.variables import destroy_variables
from .types import RequestType
from .types.behave import Context, Feature, Scenario, Status, Step
from .utils import fail_direct, in_correct_section
from .utils.protocols import mq_client_logs

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.testdata.communication import AsyncTimer

logger = logging.getLogger(__name__)

__all__ = [
    'filters',
]

try:
    import pymqi
except ModuleNotFoundError:
    from grizzly_common import dummy_pymqi as pymqi


ABORTED_RETURN_CODE = 15
IN_CORRECT_SECTION_ATTRIBUTE = 'location_status'

profile: Profile | None = None


def before_feature(context: Context, feature: Feature, *_args: Any, **_kwargs: Any) -> None:
    # identify as grizzly, instead of behave
    proc.setproctitle('grizzly')

    # <!-- silent behave.step logger, since grizzly has it's own mechanism around failing steps
    behave_logger = logging.getLogger('behave.step')
    behave_logger.setLevel(logging.CRITICAL)
    # // -->

    environ['GRIZZLY_CONTEXT_ROOT'] = context.config.base_dir
    environ['GRIZZLY_FEATURE_FILE'] = feature.filename

    destroy_variables()

    from grizzly import context as grizzly_context  # noqa: PLC0415

    grizzly_context.grizzly = GrizzlyContext()

    grizzly = grizzly_context.grizzly
    grizzly.state.verbose = context.config.verbose

    if environ.get('GRIZZLY_PROFILE', None) is not None:
        grizzly.state.profile = Profile()
        grizzly.state.profile.enable()
        logger.info('profiling enabled')

    persistent_file = Path(context.config.base_dir) / 'persistent' / f'{Path(feature.filename).stem}.json'
    persistent: dict[str, dict[str, str]] = {}

    if persistent_file.exists():
        persistent = jsonloads(persistent_file.read_text())

    context.grizzly = grizzly
    context.start = time()
    context.started = datetime.now().astimezone()
    context.last_task_count = {}
    context.exceptions = {}
    context.background_steps = getattr(feature.background, 'steps', None) or []

    # create context for all scenarios right of the bat
    for scenario in feature.scenarios:
        grizzly_scenario = grizzly.scenarios.create(scenario)
        if grizzly_scenario.class_name in persistent:
            grizzly_scenario.variables.persistent.update(persistent.get(grizzly_scenario.class_name, {}))


def validate_statistics(return_code: int, status: Status | None, context: Context, feature: Feature) -> tuple[int, set[str]]:
    """Non-worker should validate locust request statistics to determine if feature was successful or not."""
    causes: set[str] = set()

    if on_worker(context):
        return return_code, causes

    grizzly = cast('GrizzlyContext', context.grizzly)

    if not hasattr(grizzly.state, 'locust'):
        return 0, causes

    stats = grizzly.state.locust.environment.stats

    if status is None:
        status = Status.failed

    for behave_scenario in cast('list[Scenario]', feature.scenarios):
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
            if scenario_stat.num_failures > 0:
                causes.add('scenario failures')

            if scenario_stat.num_requests != grizzly_scenario.iterations:
                causes.add('incorrect number of iterations')

            if total_errors > 0:
                causes.add('error report')

            if return_code == 0:
                return_code = 1

    return return_code, causes


def after_feature(context: Context, feature: Feature, *_args: Any, **_kwargs: Any) -> None:  # noqa: PLR0912, C901, PLR0915
    return_code: int
    cause_title: str
    causes: set[str] = set()
    has_exceptions = hasattr(context, 'exceptions') and len(context.exceptions) > 0
    grizzly = cast('GrizzlyContext', context.grizzly)

    # all scenarios has been processed, let's run locust
    if feature.status == Status.passed and not has_exceptions:
        status: Status | None = None

        try:
            return_code = locustrun(context)
        except Exception as e:
            logger.exception('locust run failed')
            has_exceptions = True
            context.exceptions.update({None: [*context.exceptions.get(None, []), FeatureError(e)]})
            return_code = 1

        if return_code != 0:
            status = Status.failed if return_code != ABORTED_RETURN_CODE else Status.skipped
            feature.set_status(status)

        # optional checks, that should not be executed when it's a dry run
        if environ.get('GRIZZLY_DRY_RUN', 'false').lower() != 'true':
            return_code, causes = validate_statistics(return_code, status, context, feature)

            if pymqi.__name__ != 'grizzly_common.dummy_pymqi' and not on_worker(context):
                mq_client_logs(context)

        if return_code != 0:
            cause_title = 'locust test failed' if return_code != ABORTED_RETURN_CODE else 'locust test aborted'
    else:
        return_code = 1
        cause_title = 'failed to prepare locust test'

    if grizzly.state.profile is not None:
        grizzly.state.profile.disable()
        suffix = f'worker-{gethostname()}' if on_worker(context) else 'master'
        hprof_file = Path(context.config.base_dir) / f'grizzly-{feature.name}-{suffix}.prof'
        grizzly.state.profile.dump_stats(hprof_file)
        logger.info('profiling data saved to %s', hprof_file)

    if on_worker(context):
        return

    reporter: SummaryReporter = next(iter([possible_reporter for possible_reporter in context.config.reporters if isinstance(possible_reporter, SummaryReporter)]))

    # show start and stop date time
    stopped = datetime.now().astimezone()

    if grizzly.state.producer is not None and len(grizzly.state.producer.async_timers.timers) > 0:
        feature.set_status(Status.failed)
        timer_group: dict[str, dict[str, list[AsyncTimer]]] = {'started': {}, 'stopped': {}}

        for timer in grizzly.state.producer.async_timers.timers.values():
            group_name = 'started' if timer.stop is None else 'stopped'
            if timer.name not in timer_group[group_name]:
                timer_group[group_name].update({timer.name: []})

            timer_group[group_name][timer.name].append(timer)

        if len(timer_group['started']) > 0:
            reporter.stream.write('\nThe following asynchronous timers has not been stopped:\n')

            for name, timers in timer_group['started'].items():
                reporter.stream.write(f'- {name} ({len(timers)}):\n')

                for timer in timers:
                    reporter.stream.write(f'  * {timer.tid} (version {timer.version}): {cast("datetime", timer.start).isoformat()}\n')

        if len(timer_group['stopped']) > 0:
            reporter.stream.write('\nThe following asynchronous timers has not been started:\n')

            for name, timers in timer_group['stopped'].items():
                reporter.stream.write(f'- {name} ({len(timers)}):\n')

                for timer in timers:
                    reporter.stream.write(f'  * {timer.tid} (version {timer.version}): {cast("datetime", timer.stop).isoformat()}\n')

    if has_exceptions:
        buffer: list[str] = []

        for scenario_name, exceptions in cast('dict[str | None, list[AssertionError]]', context.exceptions).items():
            buffer_header = f'Scenario: {scenario_name}' if scenario_name is not None else ''
            buffer.extend([buffer_header] + [str(exception).replace('\n', '\n    ') for exception in exceptions] + [''])

        failure_summary = indent('\n'.join(buffer), '    ')
        reporter.stream.write(f'\nFailure summary:\n{failure_summary}')

    end_text = 'Aborted' if return_code == ABORTED_RETURN_CODE else 'Finished'

    reporter.stream.write(f'\n{"Started":<{len(end_text)}}: {context.started}\n{end_text}: {stopped}\n\n')
    reporter.stream.flush()

    # the features duration is the sum of all scenarios duration, which is the sum of all steps duration
    with suppress(Exception):
        duration = int(time() - context.start)
        feature.scenarios[-1].steps[-1].duration = duration

    if return_code != 0:
        if len(causes) > 0:
            cause_title = f'{cause_title}: {", ".join(causes)}'

        raise RuntimeError(cause_title)


def before_scenario(context: Context, scenario: Scenario, *_args: Any, **_kwargs: Any) -> None:
    grizzly = cast('GrizzlyContext', context.grizzly)

    grizzly.scenarios.select(scenario)

    if grizzly.state.background_done:
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

    if grizzly.scenario.identifier not in context.last_task_count:
        context.last_task_count[grizzly.scenario.identifier] = 0


def after_scenario(context: Context, *_args: Any, **_kwargs: Any) -> None:
    grizzly = cast('GrizzlyContext', context.grizzly)

    # first scenario is done, do not process background for any (possible) other scenarios
    if not grizzly.state.background_done:
        grizzly.state.background_done = True

    assert grizzly.scenario.tasks.tmp.async_group is None, f'async request group "{grizzly.scenario.tasks.tmp.async_group.name}" has not been closed'
    assert grizzly.scenario.tasks.tmp.loop is None, f'loop task "{grizzly.scenario.tasks.tmp.loop.name}" has not been closed'
    assert grizzly.scenario.tasks.tmp.conditional is None, f'conditional "{grizzly.scenario.tasks.tmp.conditional.name}" has not been closed'

    for scenario_name, exceptions in cast('dict[str | None, list[StepError]]', context.exceptions).items():
        if scenario_name != context.scenario.name:
            continue

        for exception in exceptions:
            exception.step.status = Status.failed


def before_step(context: Context, step: Step, *_args: Any, **_kwargs: Any) -> None:
    # fail step if it's a @backgroundsection decorated step implementation, see before_scenario hook
    with fail_direct(context):
        assert getattr(step, 'location_status', '') != 'incorrect', 'Step is in the incorrect section'

    step.in_background = step in context.background_steps

    # add current step to context, used else where
    context.step = step


def after_step(context: Context, step: Step, *_args: Any, **_kwargs: Any) -> None:
    # grizzly does not have any functionality that should run after every step, but added for
    # clarity of what can be overloaded
    grizzly = cast('GrizzlyContext', context.grizzly)

    if len(grizzly.scenario.tasks._tmp.__stack__) == 0:
        task_index = len(grizzly.scenario.tasks)
        last_task_index = context.last_task_count.get(grizzly.scenario.identifier, None)

        if last_task_index is not None and task_index > last_task_index:
            context.last_task_count[grizzly.scenario.identifier] = task_index
            # GrizzlyIterator offset by one, since it has an interal task which is not represented by a step
            grizzly.scenario.tasks.behave_steps.update({task_index + 1: f'{step.keyword} {step.name}'})
