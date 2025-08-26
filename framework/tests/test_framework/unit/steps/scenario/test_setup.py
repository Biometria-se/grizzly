"""Unit tests for grizzly.steps.scenario.setup."""

from __future__ import annotations

from contextlib import suppress
from os import environ
from typing import TYPE_CHECKING, cast

import pytest
from grizzly.exceptions import RestartScenario, RetryTask, StepError, StopUser
from grizzly.steps import *
from grizzly.tasks.clients import HttpClientTask
from grizzly.types import FailureAction, GrizzlyVariableType, RequestDirection, RequestMethod
from parse import compile as parse_compile

from test_framework.helpers import ANY

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.context import GrizzlyContext

    from test_framework.fixtures import BehaveFixture, GrizzlyFixture, MockerFixture


def test_parse_iteration_gramatical_number() -> None:
    p = parse_compile(
        'run for {iteration:d} {iteration_number:IterationGramaticalNumber}',
        extra_types={'IterationGramaticalNumber': parse_iteration_gramatical_number},
    )

    assert parse_iteration_gramatical_number.__vector__ == (False, True)

    assert p.parse('run for 1 iteration')['iteration_number'] == 'iteration'
    assert p.parse('run for 10 iterations')['iteration_number'] == 'iterations'
    assert p.parse('run for 4 laps') is None

    assert parse_iteration_gramatical_number(' asdf ') == 'asdf'


def test_parse_failure_type() -> None:
    p = parse_compile('yeehaw "{failure_type:FailureType}"', extra_types={'FailureType': parse_failure_type})

    assert p.parse('yeehaw "RuntimeError"')['failure_type'] is RuntimeError
    assert p.parse('yeehaw "RestartScenario"')['failure_type'] is RestartScenario
    assert p.parse('yeehaw "504 gateway timeout"')['failure_type'] == '504 gateway timeout'


def test_failure_action_from_step_expression() -> None:
    p = parse_compile('{failure_action:FailureAction}', extra_types={'FailureAction': FailureAction.from_step_expression})

    assert p.parse('stop user')['failure_action'] is FailureAction.STOP_USER
    assert p.parse('restart scenario')['failure_action'] is FailureAction.RESTART_SCENARIO
    assert p.parse('retry task')['failure_action'] is FailureAction.RETRY_TASK

    with pytest.raises(AssertionError, match='"foobar" is not a mapped step expression'):
        p.parse('foobar')


def test_step_setup_iterations(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast('GrizzlyContext', behave.grizzly)
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))
    behave.scenario = grizzly.scenario.behave

    assert grizzly.scenario.iterations == 1
    assert behave.exceptions == {}

    step_setup_iterations(behave, '10', 'iterations')
    assert grizzly.scenario.iterations == 10

    step_setup_iterations(behave, '1', 'iteration')
    assert grizzly.scenario.iterations == 1

    step_setup_iterations(behave, '{{ iterations }}', 'iteration')

    assert behave.exceptions == {behave.scenario.name: [ANY(AssertionError, message='variables have been found in templates, but have not been declared:\niterations')]}

    grizzly.scenario.variables['iterations'] = 100
    step_setup_iterations(behave, '{{ iterations }}', 'iteration')
    assert grizzly.scenario.iterations == 100

    step_setup_iterations(behave, '{{ iterations * 0.25 }}', 'iteration')
    assert grizzly.scenario.iterations == 25

    step_setup_iterations(behave, '0', 'iterations')
    assert grizzly.scenario.iterations == 0

    step_setup_iterations(behave, '{{ iterations / 101 }}', 'iteration')
    assert grizzly.scenario.iterations == 1

    grizzly.scenario.variables['iterations'] = 0.1
    step_setup_iterations(behave, '{{ iterations }}', 'iteration')

    assert grizzly.scenario.iterations == 1

    try:
        environ['ITERATIONS'] = '1337'

        step_setup_iterations(behave, '$env::ITERATIONS$', 'iteration')
        assert grizzly.scenario.iterations == 1337
    finally:
        with suppress(KeyError):
            del environ['ITERATIONS']

    grizzly.state.configuration['test.iterations'] = 13
    step_setup_iterations(behave, '$conf::test.iterations$', 'iterations')
    assert grizzly.scenario.iterations == 13


def test_step_setup_iteration_pace(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast('GrizzlyContext', behave.grizzly)
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))
    behave.scenario = grizzly.scenario.behave

    assert getattr(grizzly.scenario, 'pace', '') is None
    assert behave.exceptions == {}

    step_setup_iteration_pace(behave, '2000')

    assert len(grizzly.scenario.orphan_templates) == 0
    assert grizzly.scenario.pace == '2000'

    step_setup_iteration_pace(behave, 'asdf')

    assert behave.exceptions == {behave.scenario.name: [ANY(AssertionError, message='"asdf" is neither a template or a number')]}

    step_setup_iteration_pace(behave, '{{ pace }}')

    assert grizzly.scenario.orphan_templates == ['{{ pace }}']
    assert grizzly.scenario.pace == '{{ pace }}'


def test_step_setup_set_variable_alias(behave_fixture: BehaveFixture, mocker: MockerFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast('GrizzlyContext', behave.grizzly)
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))
    behave.scenario = grizzly.scenario.behave
    behave_fixture.create_step('test step', in_background=False, context=behave)

    assert grizzly.scenario.variables.alias == {}
    assert behave.exceptions == {}

    step_setup_set_variable_alias(behave, 'auth.refresh_time', 'AtomicIntegerIncrementer.test')

    assert behave.exceptions == {behave.scenario.name: [ANY(AssertionError, message='variable AtomicIntegerIncrementer.test has not been declared')]}

    step_setup_set_variable_value(behave, 'AtomicIntegerIncrementer.test', '1337')
    step_setup_set_variable_alias(behave, 'auth.refresh_time', 'AtomicIntegerIncrementer.test')

    assert grizzly.scenario.variables.alias.get('AtomicIntegerIncrementer.test', None) == 'auth.refresh_time'

    step_setup_set_variable_alias(behave, 'auth.refresh_time', 'AtomicIntegerIncrementer.test')

    assert behave.exceptions == {
        behave.scenario.name: [
            ANY(AssertionError, message='variable AtomicIntegerIncrementer.test has not been declared'),
            ANY(AssertionError, message='alias for variable AtomicIntegerIncrementer.test already exists: auth.refresh_time'),
        ],
    }

    def setitem(self: GrizzlyVariables, key: str, value: GrizzlyVariableType) -> None:
        super(GrizzlyVariables, self).__setitem__(key, value)

    mocker.patch(
        'grizzly.testdata.GrizzlyVariables.__setitem__',
        setitem,
    )

    step_setup_set_variable_alias(behave, 'auth.user.username', 'AtomicCsvReader.users.username')

    assert behave.exceptions == {
        behave.scenario.name: [
            ANY(AssertionError, message='variable AtomicIntegerIncrementer.test has not been declared'),
            ANY(AssertionError, message='alias for variable AtomicIntegerIncrementer.test already exists: auth.refresh_time'),
            ANY(AssertionError, message='variable AtomicCsvReader.users has not been declared'),
        ],
    }

    step_setup_set_variable_value(behave, 'AtomicCsvReader.users', 'users.csv')
    step_setup_set_variable_alias(behave, 'auth.user.username', 'AtomicCsvReader.users.username')
    step_setup_set_variable_alias(behave, 'auth.user.username', 'AtomicCsvReader.users.username')

    assert behave.exceptions == {
        behave.scenario.name: [
            ANY(AssertionError, message='variable AtomicIntegerIncrementer.test has not been declared'),
            ANY(AssertionError, message='alias for variable AtomicIntegerIncrementer.test already exists: auth.refresh_time'),
            ANY(AssertionError, message='variable AtomicCsvReader.users has not been declared'),
            ANY(AssertionError, message='alias for variable AtomicCsvReader.users.username already exists: auth.user.username'),
        ],
    }


def test_step_setup_log_all_requests(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast('GrizzlyContext', behave.grizzly)
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))

    assert 'log_all_requests' not in grizzly.scenario.context

    step_setup_log_all_requests(behave)

    assert grizzly.scenario.context['log_all_requests']


def test_step_setup_metadata(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast('GrizzlyContext', behave.grizzly)
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))

    assert grizzly.scenario.context.get('metadata', None) is None

    step_setup_metadata(behave, 'Content-Type', 'application/json')

    assert grizzly.scenario.context.get('metadata', None) == {
        'Content-Type': 'application/json',
    }

    step_setup_metadata(behave, 'Content-Type', 'application/xml')

    assert grizzly.scenario.context.get('metadata', None) == {
        'Content-Type': 'application/xml',
    }

    step_setup_metadata(behave, 'Ocp-Apim-Subscription-Key', 'deadbeefb00f')

    assert grizzly.scenario.context.get('metadata', None) == {
        'Content-Type': 'application/xml',
        'Ocp-Apim-Subscription-Key': 'deadbeefb00f',
    }

    grizzly.scenario.context['metadata'] = None
    request = RequestTask(RequestMethod.POST, endpoint='/api/test', name='request_task')
    grizzly.scenario.tasks.add(request)
    step_setup_metadata(behave, 'new_header', 'new_value')

    assert grizzly.scenario.context.get('metadata', {}) is None
    assert request.metadata == {'new_header': 'new_value'}

    grizzly.scenario.variables.update({'test_payload': 'none', 'test_metadata': 'none'})
    HttpClientTask.__scenario__ = grizzly.scenario
    task_factory = HttpClientTask(RequestDirection.FROM, 'http://example.org', payload_variable='test_payload', metadata_variable='test_metadata')

    grizzly.scenario.tasks.add(task_factory)
    step_setup_metadata(behave, 'x-test-header', 'foobar')

    assert grizzly.scenario.context.get('metadata', {}) is None
    assert request.metadata == {'new_header': 'new_value'}
    assert task_factory._context['metadata'] == {'x-test-header': 'foobar'}


def test_step_setup_any_failed_task_default(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast('GrizzlyContext', behave.grizzly)
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))
    behave.scenario = grizzly.scenario
    behave_fixture.create_step('test step', in_background=False, context=behave)

    assert grizzly.scenario.failure_handling == {}

    step_setup_any_failed_task_default(behave, FailureAction.STOP_USER)

    assert grizzly.scenario.failure_handling == {None: StopUser}

    step_setup_any_failed_task_default(behave, FailureAction.RESTART_SCENARIO)

    assert grizzly.scenario.failure_handling == {None: RestartScenario}

    step_setup_any_failed_task_default(behave, FailureAction.RETRY_TASK)

    assert behave.exceptions == {
        grizzly.scenario.name: [
            ANY(StepError),
        ],
    }

    exception = cast('StepError', behave.exceptions[grizzly.scenario.name][0])

    assert exception.error == 'retry task should not be used as the default behavior, only use it for specific failures'


def test_step_setup_any_failed_task_custom(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast('GrizzlyContext', behave.grizzly)
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))

    assert grizzly.scenario.failure_handling == {}

    step_setup_any_failed_task_custom(behave, RuntimeError, FailureAction.STOP_USER)

    assert grizzly.scenario.failure_handling == {RuntimeError: StopUser}

    step_setup_any_failed_task_custom(behave, RestartScenario, FailureAction.RESTART_SCENARIO)

    assert grizzly.scenario.failure_handling == {
        RuntimeError: StopUser,
        RestartScenario: RestartScenario,
    }

    step_setup_any_failed_task_custom(behave, '504 gateway timeout', FailureAction.RETRY_TASK)

    assert grizzly.scenario.failure_handling == {
        RuntimeError: StopUser,
        RestartScenario: RestartScenario,
        '504 gateway timeout': RetryTask,
    }


def test_step_setup_the_failed_task_default(grizzly_fixture: GrizzlyFixture) -> None:
    behave = grizzly_fixture.behave.context
    grizzly = grizzly_fixture.grizzly
    behave.scenario.name = grizzly.scenario.name

    task = grizzly.scenario.tasks()[-1]

    assert grizzly.scenario.failure_handling == {}
    assert task.failure_handling == {}

    step_setup_the_failed_task_default(behave, FailureAction.STOP_USER)

    assert grizzly.scenario.failure_handling == {}
    assert task.failure_handling == {None: StopUser}

    step_setup_the_failed_task_default(behave, FailureAction.RESTART_SCENARIO)

    assert grizzly.scenario.failure_handling == {}
    assert task.failure_handling == {None: RestartScenario}

    step_setup_the_failed_task_default(behave, FailureAction.RETRY_TASK)

    assert behave.exceptions == {
        grizzly.scenario.name: [
            ANY(StepError),
        ],
    }

    exception = cast('StepError', behave.exceptions[grizzly.scenario.name][0])

    assert exception.error == 'retry task should not be used as the default behavior, only use it for specific failures'
    behave.exceptions.clear()

    grizzly.scenario.tasks().clear()
    task.failure_handling.clear()

    step_setup_the_failed_task_default(behave, FailureAction.RESTART_SCENARIO)

    assert behave.exceptions == {
        grizzly.scenario.name: [
            ANY(StepError),
        ],
    }

    exception = cast('StepError', behave.exceptions[grizzly.scenario.name][0])

    assert exception.error == 'scenario does not have any tasks'


def test_step_setup_the_failed_task_custom(grizzly_fixture: GrizzlyFixture) -> None:
    behave = grizzly_fixture.behave.context
    grizzly = grizzly_fixture.grizzly
    behave.scenario.name = grizzly.scenario.name

    task = grizzly.scenario.tasks()[-1]

    assert grizzly.scenario.failure_handling == {}
    assert task.failure_handling == {}

    step_setup_the_failed_task_custom(behave, RuntimeError, FailureAction.STOP_USER)

    assert grizzly.scenario.failure_handling == {}
    assert task.failure_handling == {RuntimeError: StopUser}

    step_setup_the_failed_task_custom(behave, RestartScenario, FailureAction.RESTART_SCENARIO)

    assert grizzly.scenario.failure_handling == {}
    assert task.failure_handling == {
        RuntimeError: StopUser,
        RestartScenario: RestartScenario,
    }

    step_setup_the_failed_task_custom(behave, '504 gateway timeout', FailureAction.RETRY_TASK)

    assert grizzly.scenario.failure_handling == {}
    assert task.failure_handling == {
        RuntimeError: StopUser,
        RestartScenario: RestartScenario,
        '504 gateway timeout': RetryTask,
    }

    grizzly.scenario.tasks().clear()
    task.failure_handling.clear()

    step_setup_the_failed_task_custom(behave, '504 gateway timeout', FailureAction.RESTART_SCENARIO)

    assert behave.exceptions == {
        grizzly.scenario.name: [
            ANY(StepError),
        ],
    }

    exception = cast('StepError', behave.exceptions[grizzly.scenario.name][0])

    assert exception.error == 'scenario does not have any tasks'
