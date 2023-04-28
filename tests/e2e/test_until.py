from typing import Dict, cast
from textwrap import dedent

from grizzly.types.behave import Context, Feature
from grizzly.context import GrizzlyContext

from tests.fixtures import End2EndFixture


def test_e2e_until(e2e_fixture: End2EndFixture) -> None:
    def after_feature(context: Context, feature: Feature) -> None:
        from grizzly.locust import on_master
        if on_master(context):
            return

        grizzly = cast(GrizzlyContext, context.grizzly)

        stats = grizzly.state.locust.environment.stats

        expectations = [
            ('001 RequestTask', 'SCEN', 0, 1,),
            ('001 RequestTask', 'TSTD', 0, 1,),
            ('001 request-task', 'GET', 1, 2,),
            ('001 request-task, w=1.0s, r=2, em=1', 'UNTL', 0, 1,),
            ('002 HttpClientTask', 'SCEN', 0, 1,),
            ('002 HttpClientTask', 'TSTD', 0, 1,),
            ('002 http-client-task, w=1.0s, r=3, em=1', 'UNTL', 0, 1,),
            ('002 http-client-task', 'CLTSK', 1, 2,),
        ]

        assert len(stats.errors) == 0, f'expected 0 logged errors, got {len(stats.errors)}'

        for name, method, expected_num_failures, expected_num_requests in expectations:
            stat = stats.get(name, method)
            assert stat.num_failures == expected_num_failures, f'{stat.method}:{stat.name}.num_failures: {stat.num_failures} != {expected_num_failures}'
            assert stat.num_requests == expected_num_requests, f'{stat.method}:{stat.name}.num_requests: {stat.num_requests} != {expected_num_requests}'

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

    feature_file = e2e_fixture.create_feature(dedent(f'''Feature: UntilRequestTask
    Background: common configuration
        Given "2" users
        And spawn rate is "2" user per second
        {start_webserver_step}
    Scenario: RequestTask
        Given a user of type "RestApi" load testing "http://{e2e_fixture.host}"
        And repeat for "1" iteration
        And stop user on failure
        Then get request with name "request-task-reset" from endpoint "/api/until/reset"
        Then get request with name "request-task" from endpoint "/api/until/barbar?nth=2&wrong=foobar&right=world&as_array=true | content_type=json" until "$.`this`[?barbar="world"] | retries=2, expected_matches=1"

    Scenario: HttpClientTask
        Given a user of type "RestApi" load testing "http://{e2e_fixture.host}"
        And repeat for "1" iteration
        And stop user on failure
        Then get "http://$conf::test.host$/api/until/foofoo?nth=2&wrong=foobar&right=world&as_array=true | content_type=json" with name "http-client-task" until "$.`this`[?foofoo="world"] | retries=3, expected_matches=1"
    '''))  # noqa: E501

    rc, _ = e2e_fixture.execute(feature_file, env_conf=env_conf)

    assert rc == 0
