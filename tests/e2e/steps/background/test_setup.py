from tempfile import NamedTemporaryFile
from typing import cast, Dict, Any, List

import pytest
import yaml

from behave.runner import Context
from grizzly.context import GrizzlyContext

from ....fixtures import End2EndFixture


@pytest.mark.parametrize('url', [
    'influxdb://grizzly:password@localhost/grizzly-statistics',
    'insights://localhost/?Testplan=grizzly-statistics&InstrumentationKey=asdfasdf=',
    'influxdb://$conf::statistics.username$:$conf::statistics.password$@localhost/$conf::statistics.database$',
])
def test_e2e_step_setup_save_statistics(e2e_fixture: End2EndFixture, url: str) -> None:
    env_conf: Dict[str, Any] = {
        'configuration': {
            'statistics': {
                'username': 'grizzly',
                'password': 'password',
                'database': 'grizzly-statistics',
            }
        }
    }

    url = url.replace('localhost', e2e_fixture.host)

    def validator(context: Context) -> None:
        grizzly = cast(GrizzlyContext, context.grizzly)
        data = list(context.table)[0].as_dict()

        test_url = data.pop('url')

        for key, value in data.items():
            test_url = test_url.replace(f'$conf::{key}$', value)

        assert grizzly.setup.statistics_url == test_url, f'{grizzly.setup.statistics_url} != {test_url}'

    table: List[Dict[str, str]] = [
        {
            'url': url,
            'statistics.username': env_conf['configuration']['statistics']['username'],
            'statistics.password': env_conf['configuration']['statistics']['password'],
            'statistics.database': env_conf['configuration']['statistics']['database'],
        }
    ]

    e2e_fixture.add_validator(validator, table=table)

    feature_file = e2e_fixture.test_steps(
        background=[
            f'And save statistics to "{url}"',
        ],
        identifier=url,
    )

    with NamedTemporaryFile(delete=True, suffix='.yaml', dir=e2e_fixture.test_tmp_dir) as env_conf_file:
        env_conf_file.write(yaml.dump(env_conf, Dumper=yaml.Dumper).encode())
        env_conf_file.flush()

        rc, _ = e2e_fixture.execute(feature_file, env_conf_file=env_conf_file.name)

        assert rc == 0


@pytest.mark.parametrize('level', [
    'INFO',
    'DEBUG',
    'WARNING',
    'ERROR',
])
def test_e2e_step_setup_log_level(e2e_fixture: End2EndFixture, level: str) -> None:
    def validator(context: Context) -> None:
        grizzly = cast(GrizzlyContext, context.grizzly)
        data = list(context.table)[0].as_dict()

        test_level = data.pop('level')

        assert grizzly.setup.log_level == test_level, f'{grizzly.setup.log_level} != {test_level}'

    table: List[Dict[str, str]] = [{
        'level': level,
    }]

    e2e_fixture.add_validator(validator, table=table)

    feature_file = e2e_fixture.test_steps(
        background=[
            f'And log level is "{level}"',
        ],
        identifier=level,
    )

    rc, _ = e2e_fixture.execute(feature_file)

    assert rc == 0


@pytest.mark.parametrize('timespan', [
    '10s',
    '1h2m',
    '40m2s',
])
def test_e2e_step_setup_run_time(e2e_fixture: End2EndFixture, timespan: str) -> None:
    def validator(context: Context) -> None:
        grizzly = cast(GrizzlyContext, context.grizzly)
        data = list(context.table)[0].as_dict()

        timespan = data.pop('timespan')

        assert grizzly.setup.timespan == timespan, f'{grizzly.setup.timespan} != {timespan}'

    table: List[Dict[str, str]] = [{
        'timespan': timespan,
    }]

    e2e_fixture.add_validator(validator, table=table)

    feature_file = e2e_fixture.test_steps(
        background=[
            f'And run for maximum "{timespan}"'
        ],
        identifier=timespan,
    )

    rc, _ = e2e_fixture.execute(feature_file)

    assert rc == 0


@pytest.mark.parametrize('name,value,expected', [
    ('token.url', 'http://localhost/api/auth', '{"token": {"url": "http://localhost/api/auth"}}',),
    ('token/client id', 'aaaa-bbbb-cccc-dddd', '{"token": {"client_id": "aaaa-bbbb-cccc-dddd"}}',),
    ('log_all_requests', 'True', '{"log_all_requests": true}',),
    ('run_id', '13', '{"run_id": 13}',),
])
def test_e2e_step_setup_global_context_variable(e2e_fixture: End2EndFixture, name: str, value: str, expected: str) -> None:
    def validator(context: Context) -> None:
        from json import loads as jsonloads
        grizzly = cast(GrizzlyContext, context.grizzly)
        data = list(context.table)[0].as_dict()

        global_context = jsonloads(data['expected'])
        global_context['hello'] = {'world': 'foobar'}
        if 'token' not in global_context:
            global_context['token'] = {'client_secret': 'something'}
        else:
            global_context['token'].update({'client_secret': 'something'})

        assert grizzly.setup.global_context == global_context

    value = value.replace('localhost', e2e_fixture.host)

    table: List[Dict[str, str]] = [{
        'expected': expected.replace('localhost', e2e_fixture.host),
    }]

    e2e_fixture.add_validator(validator, table=table)

    feature_file = e2e_fixture.test_steps(
        background=[
            f'And set global context variable "{name}" to "{value}"',
            'And set global context variable "hello.world" to "foobar"',
            'And set global context variable "token/client_secret" to "something"',
        ],
        identifier=name,
    )

    rc, _ = e2e_fixture.execute(feature_file)

    assert rc == 0


@pytest.mark.parametrize('from_node,to_node,message_type', [
    ('server', 'client', 'server_to_client',),
    ('client', 'server', 'client_to_server',),
])
def test_e2e_step_setup_message_type_callback(
    e2e_fixture: End2EndFixture,
    from_node: str,
    to_node: str,
    message_type: str,
) -> None:
    def validator(context: Context) -> None:
        from grizzly.types import MessageDirection
        grizzly = cast(GrizzlyContext, context.grizzly)
        data = list(context.table)[0].as_dict()

        direction = MessageDirection.from_string(data['direction'])
        message_type = data['message_type']

        from tests.helpers import message_callback

        assert grizzly.setup.locust.messages == {
            direction: {
                message_type: message_callback,
            }
        }

        grizzly.setup.locust.messages.clear()

    table: List[Dict[str, str]] = [{
        'direction': f'{from_node}_{to_node}',
        'message_type': message_type,
    }]

    e2e_fixture.add_validator(validator, table=table)

    feature_file = e2e_fixture.test_steps(
        background=[
            f'And add callback "tests.helpers.message_callback" for message type "{message_type}" from {from_node} to {to_node}',
        ],
        identifier=message_type,
    )

    rc, _ = e2e_fixture.execute(feature_file)

    assert rc == 0
