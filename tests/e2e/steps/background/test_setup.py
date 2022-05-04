import json

from tempfile import NamedTemporaryFile
from typing import cast, Dict, Any, List

import pytest
import yaml

from behave.runner import Context
from grizzly.context import GrizzlyContext

from ....fixtures import BehaveContextFixture


@pytest.mark.parametrize('url', [
    'influxdb://grizzly:password@influx.example.com/grizzly-statistics',
    'insights://insights.example.com/?Testplan=grizzly-statistics&InstrumentationKey=asdfasdf=',
    'influxdb://$conf::statistics.username:$conf::statistics.password@influx.example.com/$conf::statistics.database',
])
def test_e2e_step_setup_save_statistics(behave_context_fixture: BehaveContextFixture, url: str) -> None:
    env_conf: Dict[str, Any] = {
        'configuration': {
            'statistics': {
                'username': 'grizzly',
                'password': 'password',
                'database': 'grizzly-statistics',
            }
        }
    }

    def validate_statistics_url(context: Context) -> None:
        grizzly = cast(GrizzlyContext, context.grizzly)
        data = list(context.table)[0].as_dict()

        test_url = data.pop('url')

        for key, value in data.items():
            test_url = test_url.replace(f'$conf::{key}', value)

        assert grizzly.setup.statistics_url == test_url, f'{grizzly.setup.statistics_url} != {test_url}'

        raise SystemExit(0)

    table: List[Dict[str, str]] = [
        {
            'url': url,
            'statistics.username': env_conf['configuration']['statistics']['username'],
            'statistics.password': env_conf['configuration']['statistics']['password'],
            'statistics.database': env_conf['configuration']['statistics']['database'],
        }
    ]

    behave_context_fixture.add_validator(validate_statistics_url, table=table)

    feature_file = behave_context_fixture.test_steps(
        background=[
            f'And save statistics to "{url}"',
        ],
    )

    with NamedTemporaryFile(delete=True, suffix='.yaml') as env_conf_file:
        env_conf_file.write(yaml.dump(env_conf, Dumper=yaml.Dumper).encode())
        env_conf_file.flush()

        rc, _ = behave_context_fixture.execute(feature_file, env_conf_file=env_conf_file.name)

        assert rc == 0


@pytest.mark.parametrize('level', [
    'INFO',
    'DEBUG',
    'WARNING',
    'ERROR',
])
def test_e2e_step_setup_log_level(behave_context_fixture: BehaveContextFixture, level: str) -> None:
    def validate_log_level(context: Context) -> None:
        grizzly = cast(GrizzlyContext, context.grizzly)
        data = list(context.table)[0].as_dict()

        test_level = data.pop('level')

        assert grizzly.setup.log_level == test_level, f'{grizzly.setup.log_level} != {test_level}'

        raise SystemExit(0)

    table: List[Dict[str, str]] = [{
        'level': level,
    }]

    behave_context_fixture.add_validator(validate_log_level, table=table)

    feature_file = behave_context_fixture.test_steps(
        background=[
            f'And log level is "{level}"',
        ],
    )

    rc, _ = behave_context_fixture.execute(feature_file)

    assert rc == 0


@pytest.mark.parametrize('timespan', [
    '10s',
    '1h2m',
    '40m2s',
    'asdf',
])
def test_e2e_step_setup_run_time(behave_context_fixture: BehaveContextFixture, timespan: str) -> None:
    def validate_run_time(context: Context) -> None:
        grizzly = cast(GrizzlyContext, context.grizzly)
        data = list(context.table)[0].as_dict()

        timespan = data.pop('timespan')

        assert grizzly.setup.timespan == timespan, f'{grizzly.setup.timespan} != {timespan}'

        raise SystemExit(0)

    table: List[Dict[str, str]] = [{
        'timespan': timespan,
    }]

    behave_context_fixture.add_validator(validate_run_time, table=table)

    feature_file = behave_context_fixture.test_steps(
        background=[
            f'And run for maximum "{timespan}"'
        ],
    )

    rc, _ = behave_context_fixture.execute(feature_file)

    assert rc == 0


@pytest.mark.parametrize('name,value,expected', [
    ('token.url', 'http://example.com/api/auth', '{"token": {"url": "http://example.com/api/auth"}}',),
    ('token/client id', 'aaaa-bbbb-cccc-dddd', '{"token": {"client_id": "aaaa-bbbb-cccc-dddd"}}',),
    ('log_all_requests', 'True', '{"log_all_requests": true}',),
    ('run_id', '13', '{"run_id": 13}',),
])
def test_e2e_step_setup_global_context_variable(behave_context_fixture: BehaveContextFixture, name: str, value: str, expected: str) -> None:
    def validate_global_context_variable(context: Context) -> None:
        grizzly = cast(GrizzlyContext, context.grizzly)
        data = list(context.table)[0].as_dict()

        global_context = json.loads(data['expected'])
        global_context['hello'] = {'world': 'foobar'}
        if 'token' not in global_context:
            global_context['token'] = {'client_secret': 'something'}
        else:
            global_context['token'].update({'client_secret': 'something'})

        assert grizzly.setup.global_context == global_context

        raise SystemExit(0)

    table: List[Dict[str, str]] = [{
        'expected': expected,
    }]

    behave_context_fixture.add_validator(validate_global_context_variable, table=table)

    feature_file = behave_context_fixture.test_steps(
        background=[
            f'And set global context variable "{name}" to "{value}"',
            'And set global context variable "hello.world" to "foobar"',
            'And set global context variable "token/client_secret" to "something"',
        ],
    )

    rc, _ = behave_context_fixture.execute(feature_file)

    assert rc == 0
