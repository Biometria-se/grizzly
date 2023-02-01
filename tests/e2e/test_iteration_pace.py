from typing import Dict, cast
from textwrap import dedent
from tempfile import NamedTemporaryFile

import yaml

from behave.runner import Context
from behave.model import Feature
from grizzly.context import GrizzlyContext

from ..fixtures import End2EndFixture


def test_e2e_iteration_pace(e2e_fixture: End2EndFixture) -> None:
    def after_feature(context: Context, feature: Feature) -> None:
        from grizzly.locust import on_master
        if on_master(context):
            return

        grizzly = cast(GrizzlyContext, context.grizzly)

        stats = grizzly.state.locust.environment.stats

        expectations = [
            ('001 RequestTask', 'SCEN', 0, 3,),
            ('001 RequestTask', 'TSTD', 0, 3,),
            ('001 RequestTask', 'PACE', 1, 3,),
            ('001 run:sleep-1', 'GET', 0, 2,),
            ('001 run:sleep-2', 'GET', 0, 1,),
        ]

        for name, method, expected_num_failures, expected_num_requests in expectations:
            stat = stats.get(name, method)
            assert stat.num_failures == expected_num_failures, f'{stat.method}:{stat.name}.num_failures: {stat.num_failures} != {expected_num_failures}'
            assert stat.num_requests == expected_num_requests, f'{stat.method}:{stat.name}.num_requests: {stat.num_requests} != {expected_num_requests}'

        stat = stats.get('001 RequestTask', 'SCEN')
        assert stat.response_times.get(500, 0) == 2

    e2e_fixture.add_after_feature(after_feature)

    if e2e_fixture._distributed:
        start_webserver_step = f'Then start webserver on master port "{e2e_fixture.webserver.port}"\n\n'
    else:
        start_webserver_step = ''

    env_conf: Dict[str, Dict[str, Dict[str, str]]] = {
        'configuration': {
            'test': {
                'host': f'http://{e2e_fixture.host}'
            }
        }
    }

    feature_file = e2e_fixture.create_feature(dedent(f'''Feature: Iteration Pace
    Background: common configuration
        Given "1" user
        And spawn rate is "1" user per second
        {start_webserver_step}
    Scenario: RequestTask
        Given a user of type "RestApi" load testing "http://{e2e_fixture.host}"
        And repeat for "3" iteration
        And set iteration time to "500" milliseconds
        And stop user on failure
        And value for variable "AtomicRandomInteger.sleep1" is "1..5"
        And value for variable "AtomicIntegerIncrementer.run_id" is "1"
        When condition "{{{{ AtomicIntegerIncrementer.run_id < 3 }}}}" with name "run" is true, execute these tasks
        Then get request with name "sleep-1" from endpoint "/api/sleep/{{{{ AtomicRandomInteger.sleep1 / 1000 / 2 }}}}"
        But if condition is false, execute these tasks
        Then get request with name "sleep-2" from endpoint "/api/sleep/0.6"
        Then end condition
    '''))  # noqa: E501

    with NamedTemporaryFile(delete=True, suffix='.yaml', dir=e2e_fixture.test_tmp_dir) as env_conf_file:
        env_conf_file.write(yaml.dump(env_conf, Dumper=yaml.Dumper).encode())
        env_conf_file.flush()

        rc, _ = e2e_fixture.execute(feature_file, env_conf_file=env_conf_file.name)

        assert rc == 0
