"""End-to-end tests of grizzly.steps.scenario.setup."""
from __future__ import annotations

import textwrap
from contextlib import suppress
from typing import TYPE_CHECKING, Any, cast

import pytest

from grizzly.context import GrizzlyContext

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.types.behave import Context
    from tests.fixtures import End2EndFixture


def test_e2e_step_setup_set_context_variable(e2e_fixture: End2EndFixture) -> None:
    testdata = [
        ('token.url', 'http://example.com/api/auth', '{"token": {"url": "http://example.com/api/auth"}}'),
        ('token/client id', 'aaaa-bbbb-cccc-dddd', '{"token": {"client_id": "aaaa-bbbb-cccc-dddd"}}'),
        ('log_all_requests', 'True', '{"log_all_requests": true}'),
        ('run_id', '13', '{"run_id": 13}'),
        ('www.example.com/auth.user.username', 'bob', '{"www.example.com": {"auth": {"user": {"username": "bob"}}}}'),
    ]

    def validate_context_variable(context: Context) -> None:
        from contextlib import suppress
        from json import loads as jsonloads

        from grizzly.utils import merge_dicts

        grizzly = cast(GrizzlyContext, context.grizzly)
        expected_total: dict[str, Any] = {}
        first_row = next(iter(context.table)).as_dict()
        expected_host = first_row['expected']

        for row in list(context.table)[1:]:
            data = row.as_dict()

            expected = jsonloads(data['expected'])
            expected['hello'] = {'world': 'foobar'}

            if 'token' not in expected:
                expected['token'] = {'client_secret': 'something'}
            else:
                expected['token'].update({'client_secret': 'something'})

            expected_total = merge_dicts(expected_total, expected)

        actual = grizzly.scenario.context.copy()

        with suppress(KeyError):
            del actual['host']

        assert actual == expected_total, f'{actual!s} != {expected!s}'
        assert grizzly.scenario.context.get('host', None) == f'http://{expected_host}'  # added by fixture

    table: list[dict[str, str]] = [{'expected': e2e_fixture.host}]
    scenario: list[str] = []

    for name, value, expected in testdata:
        table.append({'expected': expected})
        scenario.append(f'And set context variable "{name}" to "{value}"')

    e2e_fixture.add_validator(validate_context_variable, table=table)

    feature_file = e2e_fixture.test_steps(
        scenario=[
            *scenario,
            'And set context variable "hello.world" to "foobar"',
            'And set context variable "token/client_secret" to "something"',
        ],
        identifier=name,
    )

    rc, _ = e2e_fixture.execute(feature_file)

    assert rc == 0


# no easy way to rewrite without parameterize without rewriting End2EndFixture...
@pytest.mark.parametrize('iterations', [
    '10', '1', '{{ leveranser * 0.25 }}',
])
def test_e2e_step_setup_iterations(e2e_fixture: End2EndFixture, iterations: str) -> None:
    def validate_iterations(context: Context) -> None:
        grizzly = cast(GrizzlyContext, context.grizzly)
        data = next(iter(context.table)).as_dict()

        iterations = int(data['iterations'].replace('{{ leveranser * 0.25 }}', '25'))

        assert grizzly.scenario.iterations == iterations, f'{grizzly.scenario.iterations} != {iterations}'

    table: list[dict[str, str]] = [{
        'iterations': iterations,
    }]

    suffix = 's'
    with suppress(Exception):
        if int(iterations) <= 1:
            suffix = ''

    e2e_fixture.add_validator(validate_iterations, table=table)

    feature_file = e2e_fixture.test_steps(
        background=[
            'Then ask for value of variable "leveranser"',
        ],
        scenario=[
            f'And repeat for "{iterations}" iteration{suffix}',
            'Then log message "leveranser={{ leveranser }}"',
        ],
        identifier=iterations,
    )

    rc, _ = e2e_fixture.execute(feature_file, testdata={'leveranser': '100'})

    assert rc == 0


# no easy way to rewrite without parameterize without rewriting End2EndFixture...
@pytest.mark.parametrize('pace', ['2000', '{{ pace }}'])
def test_e2e_step_setup_pace(e2e_fixture: End2EndFixture, pace: str) -> None:
    def validate_iterations(context: Context) -> None:
        from grizzly.utils import has_template

        grizzly = cast(GrizzlyContext, context.grizzly)
        data = next(iter(context.table)).as_dict()
        pace = data['pace']

        assert grizzly.scenario.pace == pace, f'{grizzly.scenario.pace} != {pace}'

        if has_template(pace):
            for scenario in grizzly.scenarios:
                assert pace in scenario.orphan_templates, f'"{pace}" not in {scenario.orphan_templates}'

    table: list[dict[str, str]] = [{
        'pace': pace,
    }]

    e2e_fixture.add_validator(validate_iterations, table=table)

    feature_file = e2e_fixture.test_steps(
        background=[
            'Then ask for value of variable "pace"',
        ],
        scenario=[
            f'And set iteration time to "{pace}" milliseconds',
            'Then log message "pace={{ pace }}"',
        ],
        identifier=pace,
    )

    rc, _ = e2e_fixture.execute(feature_file, testdata={'pace': '2000'})

    assert rc == 0


def test_e2e_step_set_variable_alias(e2e_fixture: End2EndFixture) -> None:
    def validate_variable_alias(context: Context) -> None:
        grizzly = cast(GrizzlyContext, context.grizzly)
        grizzly.scenario.tasks.pop()

        alias = grizzly.scenario.variables.alias

        assert len(alias) == 2, 'unexpected number of aliases'
        alias_username = alias.get('AtomicCsvReader.users.username', None)
        assert alias_username == 'auth.user.username', f'{alias_username} != auth.user.username'
        alias_password = alias.get('AtomicCsvReader.users.password', None)
        assert alias_password == 'auth.user.password', f'{alias_password} != auth.user.password'  # noqa: S105
        variable = grizzly.scenario.variables.get('AtomicCsvReader.users', None)
        assert variable == 'users.csv | repeat=True', f'{variable} != users.csv | repeat=True'

    e2e_fixture.add_validator(validate_variable_alias)

    feature_file = e2e_fixture.test_steps(
        scenario=[
            'And value for variable "AtomicCsvReader.users" is "users.csv | repeat=True"',
            'And set alias "auth.user.username" for variable "AtomicCsvReader.users.username"',
            'And set alias "auth.user.password" for variable "AtomicCsvReader.users.password"',
            'Then log message "username={{ AtomicCsvReader.users.username }}"',
            'Then log message "password={{ AtomicCsvReader.users.password }}"',
        ],
    )

    (e2e_fixture.root / 'features' / 'requests' / 'users.csv').write_text(textwrap.dedent(
        """username,password
grizzly,secret""",
    ))

    rc, output = e2e_fixture.execute(feature_file)

    result = ''.join(output)

    assert rc == 0
    assert 'username=grizzly' in result
    assert 'password=secret' in result


def test_e2e_step_setup_log_all_requests(e2e_fixture: End2EndFixture) -> None:
    def validate_log_all_requests(context: Context) -> None:
        grizzly = cast(GrizzlyContext, context.grizzly)

        assert grizzly.scenario.context.get('log_all_requests', False)

    e2e_fixture.add_validator(validate_log_all_requests)

    feature_file = e2e_fixture.test_steps(
        scenario=[
            'And log all requests',
        ],
    )

    rc, _ = e2e_fixture.execute(feature_file)

    assert rc == 0


def test_e2e_step_setup_stop_user_on_failure(e2e_fixture: End2EndFixture) -> None:
    def validate_stop_user_on_failure(context: Context) -> None:
        from grizzly.types.locust import StopUser
        grizzly = cast(GrizzlyContext, context.grizzly)

        assert grizzly.scenario.failure_handling.get(None, None) is not None
        assert isinstance(grizzly.scenario.failure_handling.get(None, RuntimeError)(), StopUser), 'failure exception is not StopUser'

    e2e_fixture.add_validator(validate_stop_user_on_failure)

    feature_file = e2e_fixture.test_steps(
        scenario=[
            'And stop user on failure',
        ],
    )

    rc, _ = e2e_fixture.execute(feature_file)

    assert rc == 0


def test_e2e_step_setup_restart_scenario_on_failure(e2e_fixture: End2EndFixture) -> None:
    def validate_restart_scenario_on_failure(context: Context) -> None:
        from grizzly.exceptions import RestartScenario
        grizzly = cast(GrizzlyContext, context.grizzly)

        assert grizzly.scenario.failure_handling.get(None, None) is not None
        assert isinstance(grizzly.scenario.failure_handling.get(None, RuntimeError)(), RestartScenario), 'failure exception is not RestartScenario'

    e2e_fixture.add_validator(validate_restart_scenario_on_failure)

    feature_file = e2e_fixture.test_steps(
        scenario=[
            'And restart scenario on failure',
        ],
    )

    rc, _ = e2e_fixture.execute(feature_file)

    assert rc == 0


def test_e2e_setup_metadata(e2e_fixture: End2EndFixture) -> None:
    def validate_metadata(context: Context) -> None:
        grizzly = cast(GrizzlyContext, context.grizzly)
        assert grizzly.scenario.variables.get('nested_value', None) == 10, 'nested_value variable is not 10'

        metadata = grizzly.scenario.context.get('metadata', None)
        assert metadata == {
            'Content-Type': 'application/xml',
            'Ocp-Apim-Subscription-Key': '9asdf00asdf00adsf034',
            'nested': 10,
        }, f'unexpected metadata: {metadata!s}'

    e2e_fixture.add_validator(validate_metadata)

    feature_file = e2e_fixture.test_steps(
        scenario=[
            'And value for variable "nested_value" is "10"',
            'And metadata "Content-Type" is "application/xml"',
            'And metadata "Ocp-Apim-Subscription-Key" is "9asdf00asdf00adsf034"',
            'And metadata "nested" is "{{ nested_value }}"',
            'Then log message "nested_value={{ nested_value }}"',
        ],
    )

    rc, _ = e2e_fixture.execute(feature_file)

    assert rc == 0


def test_e2e_setup_failed_task(e2e_fixture: End2EndFixture) -> None:
    def validate_failure_handling(context: Context) -> None:
        from grizzly.exceptions import RestartScenario, RetryTask, StopUser

        grizzly = cast(GrizzlyContext, context.grizzly)

        assert grizzly.scenario.failure_handling == {
            None: RestartScenario,
            '504 gateway timeout': RetryTask,
            MemoryError: StopUser,
        }

    e2e_fixture.add_validator(validate_failure_handling)

    feature_file = e2e_fixture.test_steps(
        scenario=[
            'When a task fails restart scenario',
            'When a task fails with "504 gateway timeout" retry task',
            'When a task fails with "MemoryError" stop user',
        ],
    )

    rc, _ = e2e_fixture.execute(feature_file)

    assert rc == 0
