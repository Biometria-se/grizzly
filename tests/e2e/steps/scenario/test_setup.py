import json
import textwrap

from typing import cast, List, Dict
import pytest

from behave.runner import Context
from grizzly.context import GrizzlyContext

from ....fixtures import BehaveContextFixture


@pytest.mark.parametrize('name,value,expected', [
    ('token.url', 'http://example.com/api/auth', '{"token": {"url": "http://example.com/api/auth"}}',),
    ('token/client id', 'aaaa-bbbb-cccc-dddd', '{"token": {"client_id": "aaaa-bbbb-cccc-dddd"}}',),
    ('log_all_requests', 'True', '{"log_all_requests": true}',),
    ('run_id', '13', '{"run_id": 13}',),
])
def test_e2e_step_setup_set_context_variable(behave_context_fixture: BehaveContextFixture, name: str, value: str, expected: str) -> None:
    def validate_context_variable(context: Context) -> None:
        grizzly = cast(GrizzlyContext, context.grizzly)
        data = list(context.table)[0].as_dict()

        expected = json.loads(data['expected'])
        expected['hello'] = {'world': 'foobar'}
        if 'token' not in expected:
            expected['token'] = {'client_secret': 'something'}
        else:
            expected['token'].update({'client_secret': 'something'})

        actual = grizzly.scenario.context.copy()

        try:
            del actual['host']
        except KeyError:
            pass

        assert actual == expected, f'{str(actual)} != {str(expected)}'
        assert grizzly.scenario.context.get('host', None) == 'http://localhost:1'  # added by fixture

        raise SystemExit(0)

    table: List[Dict[str, str]] = [{
        'expected': expected,
    }]

    behave_context_fixture.add_validator(validate_context_variable, table=table)

    feature_file = behave_context_fixture.test_steps(
        scenario=[
            f'And set context variable "{name}" to "{value}"',
            'And set context variable "hello.world" to "foobar"',
            'And set context variable "token/client_secret" to "something"',
        ],
        identifier=name,
    )

    rc, _ = behave_context_fixture.execute(feature_file)

    assert rc == 0


@pytest.mark.parametrize('iterations', [
    '10', '1', '{{ leveranser * 0.25 }}',
])
def test_e2e_step_setup_iterations(behave_context_fixture: BehaveContextFixture, iterations: str) -> None:
    def validate_iterations(context: Context) -> None:
        grizzly = cast(GrizzlyContext, context.grizzly)
        data = list(context.table)[0].as_dict()

        iterations = int(data['iterations'].replace('{{ leveranser * 0.25 }}', '25'))

        assert grizzly.scenario.iterations == iterations, f'{grizzly.scenario.iterations} != {iterations}'

        raise SystemExit(0)

    table: List[Dict[str, str]] = [{
        'iterations': iterations,
    }]

    suffix = 's'
    try:
        if int(iterations) <= 1:
            suffix = ''
    except:
        pass

    behave_context_fixture.add_validator(validate_iterations, table=table)

    feature_file = behave_context_fixture.test_steps(
        background=[
            'Then ask for value of variable "leveranser"',
        ],
        scenario=[
            f'And repeat for "{iterations}" iteration{suffix}'
        ],
        identifier=iterations,
    )

    rc, _ = behave_context_fixture.execute(feature_file, testdata={'leveranser': '100'})

    assert rc == 0


def test_e2e_step_setup_wait_time(behave_context_fixture: BehaveContextFixture) -> None:
    def validate_wait_time(context: Context) -> None:
        grizzly = cast(GrizzlyContext, context.grizzly)

        assert grizzly.scenario.wait.minimum == 0.3
        assert grizzly.scenario.wait.maximum == 0.5

        raise SystemExit(0)

    behave_context_fixture.add_validator(validate_wait_time)

    feature_file = behave_context_fixture.test_steps(
        scenario=[
            'And wait time inbetween requests is random between "0.3" and "0.5" seconds',
        ],
    )

    rc, _ = behave_context_fixture.execute(feature_file)

    assert rc == 0


def test_e2e_step_variable_value(behave_context_fixture: BehaveContextFixture) -> None:
    def validate_variable_value(context: Context) -> None:
        grizzly = cast(GrizzlyContext, context.grizzly)

        assert grizzly.state.variables.get('testdata_variable', None) == 'hello world!'
        assert grizzly.state.variables.get('int_value', None) == 10
        assert grizzly.state.variables.get('float_value', None) == 1.0
        assert grizzly.state.variables.get('bool_value', False)
        assert grizzly.state.variables.get('wildcard', None) == 'foobar'
        assert grizzly.state.variables.get('nested_value', None) == 'hello world!'

        raise SystemExit(0)

    behave_context_fixture.add_validator(validate_variable_value)

    feature_file = behave_context_fixture.test_steps(
        scenario=[
            'And value for variable "testdata_variable" is "hello world!"',
            'And value for variable "int_value" is "10"',
            'And value for variable "float_value" is "1.0"',
            'And value for variable "bool_value" is "True"',
            'And value for variable "wildcard" is "foobar"',
            'And value for variable "nested_value" is "{{ testdata_variable }}"',
        ],
    )

    rc, _ = behave_context_fixture.execute(feature_file)

    assert rc == 0


def test_e2e_step_set_variable_alias(behave_context_fixture: BehaveContextFixture) -> None:
    def validate_variable_alias(context: Context) -> None:
        grizzly = cast(GrizzlyContext, context.grizzly)

        alias = grizzly.state.alias

        assert len(alias) == 2, 'unexpected number of aliases'
        alias_username = alias.get('AtomicCsvRow.users.username', None)
        assert alias_username == 'auth.user.username', f'{alias_username} != auth.user.username'
        alias_password = alias.get('AtomicCsvRow.users.password', None)
        assert alias_password == 'auth.user.password', f'{alias_password} != auth.user.password'
        variable = grizzly.state.variables.get('AtomicCsvRow.users', None)
        assert variable == 'users.csv | repeat=True', f'{variable} != users.csv | repeat=True'

        raise SystemExit(0)

    behave_context_fixture.add_validator(validate_variable_alias)

    feature_file = behave_context_fixture.test_steps(
        scenario=[
            'And value for variable "AtomicCsvRow.users" is "users.csv | repeat=True"',
            'And set alias "auth.user.username" for variable "AtomicCsvRow.users.username"',
            'And set alias "auth.user.password" for variable "AtomicCsvRow.users.password"',
        ],
    )

    (behave_context_fixture.root / 'features' / 'requests' / 'users.csv').write_text(textwrap.dedent(
        '''username,password
        grizzly,secret
        '''
    ))

    rc, _ = behave_context_fixture.execute(feature_file)

    assert rc == 0


def test_e2e_step_setup_log_all_requests(behave_context_fixture: BehaveContextFixture) -> None:
    def validate_log_all_requests(context: Context) -> None:
        grizzly = cast(GrizzlyContext, context.grizzly)

        assert grizzly.scenario.context.get('log_all_requests', False)

        raise SystemExit(0)

    behave_context_fixture.add_validator(validate_log_all_requests)

    feature_file = behave_context_fixture.test_steps(
        scenario=[
            'And log all requests',
        ]
    )

    rc, _ = behave_context_fixture.execute(feature_file)

    assert rc == 0


def test_e2e_step_setup_stop_user_on_failure(behave_context_fixture: BehaveContextFixture) -> None:
    def validate_stop_user_on_failure(context: Context) -> None:
        from locust.exception import StopUser
        grizzly = cast(GrizzlyContext, context.grizzly)

        assert grizzly.scenario.failure_exception is not None
        assert isinstance(grizzly.scenario.failure_exception(), StopUser), 'failure exception is not StopUser'

        raise SystemExit(0)

    behave_context_fixture.add_validator(validate_stop_user_on_failure)

    feature_file = behave_context_fixture.test_steps(
        scenario=[
            'And stop user on failure',
        ]
    )

    rc, _ = behave_context_fixture.execute(feature_file)

    assert rc == 0


def test_e2e_step_setup_restart_scenario_on_failure(behave_context_fixture: BehaveContextFixture) -> None:
    def validate_restart_scenario_on_failure(context: Context) -> None:
        from grizzly.exceptions import RestartScenario
        grizzly = cast(GrizzlyContext, context.grizzly)

        assert grizzly.scenario.failure_exception is not None
        assert isinstance(grizzly.scenario.failure_exception(), RestartScenario), 'failure exception is not RestartScenario'

        raise SystemExit(0)

    behave_context_fixture.add_validator(validate_restart_scenario_on_failure)

    feature_file = behave_context_fixture.test_steps(
        scenario=[
            'And restart scenario on failure',
        ]
    )

    rc, _ = behave_context_fixture.execute(feature_file)

    assert rc == 0


def test_e2e_setup_metadata(behave_context_fixture: BehaveContextFixture) -> None:
    def validate_metadata(context: Context) -> None:
        grizzly = cast(GrizzlyContext, context.grizzly)
        assert grizzly.state.variables.get('nested_value', None) == 10, 'nested_value variable is not 10'

        metadata = grizzly.scenario.context.get('metadata', None)
        assert metadata == {
            'Content-Type': 'application/xml',
            'Ocp-Apim-Subscription-Key': '9asdf00asdf00adsf034',
            'nested': 10,
        }, f'unexpected metadata: {str(metadata)}'

        raise SystemExit(0)

    behave_context_fixture.add_validator(validate_metadata)

    feature_file = behave_context_fixture.test_steps(
        scenario=[
            'And value for variable "nested_value" is "10"',
            'And metadata "Content-Type" is "application/xml"',
            'And metadata "Ocp-Apim-Subscription-Key" is "9asdf00asdf00adsf034"',
            'And metadata "nested" is "{{ nested_value }}"',
        ],
    )

    rc, _ = behave_context_fixture.execute(feature_file)

    assert rc == 0
