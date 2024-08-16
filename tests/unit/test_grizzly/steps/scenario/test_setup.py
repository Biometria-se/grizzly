"""Unit tests for grizzly.steps.scenario.setup."""
from __future__ import annotations

from contextlib import suppress
from os import environ
from typing import TYPE_CHECKING, cast

from parse import compile

from grizzly.context import GrizzlyContext
from grizzly.steps import *
from grizzly.tasks.clients import HttpClientTask
from grizzly.testdata import GrizzlyVariables, GrizzlyVariableType
from grizzly.types import RequestDirection, RequestMethod
from tests.helpers import ANY

if TYPE_CHECKING:  # pragma: no cover
    from pytest_mock import MockerFixture

    from tests.fixtures import BehaveFixture


def test_parse_iteration_gramatical_number() -> None:
    p = compile(
        'run for {iteration:d} {iteration_number:IterationGramaticalNumber}',
        extra_types={'IterationGramaticalNumber': parse_iteration_gramatical_number},
    )

    assert parse_iteration_gramatical_number.__vector__ == (False, True)

    assert p.parse('run for 1 iteration')['iteration_number'] == 'iteration'
    assert p.parse('run for 10 iterations')['iteration_number'] == 'iterations'
    assert p.parse('run for 4 laps') is None

    assert parse_iteration_gramatical_number(' asdf ') == 'asdf'


def test_step_setup_iterations(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast(GrizzlyContext, behave.grizzly)
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))
    behave.scenario = grizzly.scenario.behave

    assert grizzly.scenario.iterations == 1
    assert behave.exceptions == {}

    step_setup_iterations(behave, '10', 'iterations')
    assert grizzly.scenario.iterations == 10

    step_setup_iterations(behave, '1', 'iteration')
    assert grizzly.scenario.iterations == 1

    step_setup_iterations(behave, '{{ iterations }}', 'iteration')

    assert behave.exceptions == {behave.scenario.name: [ANY(AssertionError, message='value contained variable "iterations" which has not been declared')]}

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


def test_step_setup_pace(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast(GrizzlyContext, behave.grizzly)
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))
    behave.scenario = grizzly.scenario.behave

    assert getattr(grizzly.scenario, 'pace', '') is None
    assert behave.exceptions == {}

    step_setup_pace(behave, '2000')

    assert len(grizzly.scenario.orphan_templates) == 0
    assert grizzly.scenario.pace == '2000'

    step_setup_pace(behave, 'asdf')

    assert behave.exceptions == {behave.scenario.name: [ANY(AssertionError, message='"asdf" is neither a template or a number')]}

    step_setup_pace(behave, '{{ pace }}')

    assert grizzly.scenario.orphan_templates == ['{{ pace }}']
    assert grizzly.scenario.pace == '{{ pace }}'


def test_step_setup_set_variable_alias(behave_fixture: BehaveFixture, mocker: MockerFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast(GrizzlyContext, behave.grizzly)
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))
    behave.scenario = grizzly.scenario.behave
    behave_fixture.create_step('test step', in_background=False, context=behave)

    assert grizzly.scenario.variables.alias == {}
    assert behave.exceptions == {}

    step_setup_set_variable_alias(behave, 'auth.refresh_time', 'AtomicIntegerIncrementer.test')

    assert behave.exceptions == {behave.scenario.name: [ANY(AssertionError, message='variable AtomicIntegerIncrementer.test has not been declared')]}

    step_setup_variable_value(behave, 'AtomicIntegerIncrementer.test', '1337')
    step_setup_set_variable_alias(behave, 'auth.refresh_time', 'AtomicIntegerIncrementer.test')

    assert grizzly.scenario.variables.alias.get('AtomicIntegerIncrementer.test', None) == 'auth.refresh_time'

    step_setup_set_variable_alias(behave, 'auth.refresh_time', 'AtomicIntegerIncrementer.test')

    assert behave.exceptions == {behave.scenario.name: [
        ANY(AssertionError, message='variable AtomicIntegerIncrementer.test has not been declared'),
        ANY(AssertionError, message='alias for variable AtomicIntegerIncrementer.test already exists: auth.refresh_time'),
    ]}

    def setitem(self: GrizzlyVariables, key: str, value: GrizzlyVariableType) -> None:
        super(GrizzlyVariables, self).__setitem__(key, value)

    mocker.patch(
        'grizzly.testdata.GrizzlyVariables.__setitem__',
        setitem,
    )

    step_setup_set_variable_alias(behave, 'auth.user.username', 'AtomicCsvReader.users.username')

    assert behave.exceptions == {behave.scenario.name: [
        ANY(AssertionError, message='variable AtomicIntegerIncrementer.test has not been declared'),
        ANY(AssertionError, message='alias for variable AtomicIntegerIncrementer.test already exists: auth.refresh_time'),
        ANY(AssertionError, message='variable AtomicCsvReader.users has not been declared'),
    ]}

    step_setup_variable_value(behave, 'AtomicCsvReader.users', 'users.csv')
    step_setup_set_variable_alias(behave, 'auth.user.username', 'AtomicCsvReader.users.username')
    step_setup_set_variable_alias(behave, 'auth.user.username', 'AtomicCsvReader.users.username')

    assert behave.exceptions == {behave.scenario.name: [
        ANY(AssertionError, message='variable AtomicIntegerIncrementer.test has not been declared'),
        ANY(AssertionError, message='alias for variable AtomicIntegerIncrementer.test already exists: auth.refresh_time'),
        ANY(AssertionError, message='variable AtomicCsvReader.users has not been declared'),
        ANY(AssertionError, message='alias for variable AtomicCsvReader.users.username already exists: auth.user.username'),
    ]}


def test_step_setup_log_all_requests(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast(GrizzlyContext, behave.grizzly)
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))

    assert 'log_all_requests' not in grizzly.scenario.context

    step_setup_log_all_requests(behave)

    assert grizzly.scenario.context['log_all_requests']


def test_step_setup_metadata(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast(GrizzlyContext, behave.grizzly)
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
