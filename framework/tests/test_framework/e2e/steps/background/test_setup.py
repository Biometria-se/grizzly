"""End-to-end test cases for grizzly.steps.background.setup."""

from __future__ import annotations

import inspect
from typing import TYPE_CHECKING, cast

import pytest

from test_framework.helpers import message_callback

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.context import GrizzlyContext
    from grizzly.types import StrDict
    from grizzly.types.behave import Context

    from test_framework.fixtures import End2EndFixture


@pytest.mark.parametrize(
    'url',
    [
        'influxdb://grizzly:password@localhost/grizzly-statistics?Testplan=grizzly-statistics',
        'influxdb2://token@localhost:31337/org:bucket?Testplan=grizzly-statistics',
        'influxdb://$conf::statistics.username$:$conf::statistics.password$@localhost/$conf::statistics.database$?Testplan=grizzly-statistics',
        'influxdb2://$conf::statistics.token$@localhost:31337/$conf::statistics.org$:$conf::statistics.bucket$?Testplan=grizzly-statistics',
    ],
)
def test_e2e_step_setup_save_statistics(e2e_fixture: End2EndFixture, url: str) -> None:
    env_conf: StrDict = {
        'configuration': {
            'statistics': {
                'username': 'grizzly',
                'password': 'password',
                'database': 'grizzly-statistics',
                'token': 'token',
                'org': 'ifk',
                'bucket': 'grizzly',
            },
        },
    }

    url = url.replace('localhost', e2e_fixture.host)

    def validator(context: Context) -> None:
        grizzly = cast('GrizzlyContext', context.grizzly)
        data = next(iter(context.table)).as_dict()

        test_url = data.pop('url')

        for key, value in data.items():
            test_url = test_url.replace(f'$conf::{key}$', value)

        assert grizzly.setup.statistics_url == test_url, f'{grizzly.setup.statistics_url} != {test_url}'

    table: list[dict[str, str]] = [
        {
            'url': url,
            'statistics.username': env_conf['configuration']['statistics']['username'],
            'statistics.password': env_conf['configuration']['statistics']['password'],
            'statistics.database': env_conf['configuration']['statistics']['database'],
            'statistics.token': env_conf['configuration']['statistics']['token'],
            'statistics.org': env_conf['configuration']['statistics']['org'],
            'statistics.bucket': env_conf['configuration']['statistics']['bucket'],
        },
    ]

    e2e_fixture.add_validator(validator, table=table)

    feature_file = e2e_fixture.test_steps(
        background=[
            f'And save statistics to "{url}"',
        ],
        identifier=url,
    )

    rc, output = e2e_fixture.execute(feature_file, env_conf=env_conf)

    try:
        assert rc == 0
    except AssertionError:
        print(''.join(output))
        raise


@pytest.mark.parametrize(
    'level',
    [
        'INFO',
        'DEBUG',
        'WARNING',
        'ERROR',
    ],
)
def test_e2e_step_setup_log_level(e2e_fixture: End2EndFixture, level: str) -> None:
    def validator(context: Context) -> None:
        grizzly = cast('GrizzlyContext', context.grizzly)
        data = next(iter(context.table)).as_dict()

        test_level = data.pop('level')

        assert grizzly.setup.log_level == test_level, f'{grizzly.setup.log_level} != {test_level}'

    table: list[dict[str, str]] = [
        {
            'level': level,
        },
    ]

    e2e_fixture.add_validator(validator, table=table)

    feature_file = e2e_fixture.test_steps(
        background=[
            f'And log level is "{level}"',
        ],
        identifier=level,
    )

    rc, _ = e2e_fixture.execute(feature_file)

    assert rc == 0


@pytest.mark.parametrize(
    'timespan',
    [
        '10s',
        '1h2m',
        '40m2s',
    ],
)
def test_e2e_step_setup_run_time(e2e_fixture: End2EndFixture, timespan: str) -> None:
    def validator(context: Context) -> None:
        grizzly = cast('GrizzlyContext', context.grizzly)
        data = next(iter(context.table)).as_dict()

        timespan = data.pop('timespan')

        assert grizzly.setup.timespan == timespan, f'{grizzly.setup.timespan} != {timespan}'

    table: list[dict[str, str]] = [
        {
            'timespan': timespan,
        },
    ]

    e2e_fixture.add_validator(validator, table=table)

    feature_file = e2e_fixture.test_steps(
        background=[
            f'And run for maximum "{timespan}"',
        ],
        identifier=timespan,
    )

    rc, _ = e2e_fixture.execute(feature_file)

    assert rc == 0


@pytest.mark.parametrize(
    ('from_node', 'to_node', 'message_type'),
    [
        ('server', 'client', 'server_to_client'),
        ('client', 'server', 'client_to_server'),
    ],
)
def test_e2e_step_setup_message_type_callback(
    e2e_fixture: End2EndFixture,
    from_node: str,
    to_node: str,
    message_type: str,
) -> None:
    def validator(context: Context) -> None:
        from grizzly.types import MessageDirection

        grizzly = cast('GrizzlyContext', context.grizzly)
        data = next(iter(context.table)).as_dict()

        direction = MessageDirection.from_string(data['direction'])
        message_type = data['message_type']

        from steps.helpers import message_callback  # type: ignore  # noqa: PGH003

        assert grizzly.setup.locust.messages == {
            direction: {
                message_type: message_callback,
            },
        }

        grizzly.setup.locust.messages.clear()

    source = inspect.getsource(message_callback)
    (e2e_fixture.root / 'features' / 'steps' / 'helpers.py').write_text(f"""from grizzly.types.locust import Message, Environment

{source}
""")

    table: list[dict[str, str]] = [
        {
            'direction': f'{from_node}_{to_node}',
            'message_type': message_type,
        },
    ]

    e2e_fixture.add_validator(validator, table=table)

    feature_file = e2e_fixture.test_steps(
        background=[
            f'And register callback "steps.helpers.message_callback" for message type "{message_type}" from {from_node} to {to_node}',
        ],
        identifier=message_type,
    )

    rc, _ = e2e_fixture.execute(feature_file)

    assert rc == 0


@pytest.mark.parametrize(
    'timeout',
    [
        None,
        10.0,
    ],
)
def test_e2e_step_setup_wait_spawning_complete(e2e_fixture: End2EndFixture, timeout: float | None) -> None:
    def validator(context: Context) -> None:
        grizzly = cast('GrizzlyContext', context.grizzly)
        data = next(iter(context.table)).as_dict()

        timeout = data.pop('timeout')

        value = -1 if timeout == 'None' else float(timeout)

        assert grizzly.setup.wait_for_spawning_complete == value, f'{grizzly.setup.wait_for_spawning_complete} != {value}'

    table: list[dict[str, str]] = [
        {
            'timeout': f'{timeout!r}',
        },
    ]

    e2e_fixture.add_validator(validator, table=table)

    step = 'Given wait until spawning is complete' if timeout is None else f'Given wait "{timeout}" seconds until spawning is complete'

    feature_file = e2e_fixture.test_steps(
        background=[
            step,
        ],
        identifier=f'{timeout!r}',
    )

    rc, _ = e2e_fixture.execute(feature_file)

    assert rc == 0
