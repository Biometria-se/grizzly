from typing import Dict, cast
from textwrap import dedent

from grizzly.context import GrizzlyContext
from grizzly.types.behave import Context, Feature

from tests.fixtures import End2EndFixture


def test_e2e_iteration_pace(e2e_fixture: End2EndFixture) -> None:
    def after_feature(context: Context, feature: Feature) -> None:
        from grizzly.locust import on_worker
        if on_worker(context):
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
        Given "2" user
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
    Scenario: Dummy
        Given a user of type "RestApi" load testing "http://{e2e_fixture.host}"
        And repeat for "3" iteration
        Then log message "dummy"
    '''))  # noqa: E501

    rc, output = e2e_fixture.execute(feature_file, env_conf=env_conf)

    result = ''.join(output)

    assert rc == 1
    assert 'HOOK-ERROR in after_feature: RuntimeError: locust test failed'
    if e2e_fixture._distributed:
        assert "1                  PACE 001 RequestTask: \"RuntimeError('pace falling behind')\"" in result  # ?!?!?!
    else:
        assert "1                  PACE 001 RequestTask: RuntimeError('pace falling behind')" in result
