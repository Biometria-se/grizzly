from textwrap import dedent
from typing import cast

from grizzly.types.behave import Context, Feature
from grizzly.context import GrizzlyContext

from tests.fixtures import End2EndFixture


def test_e2e_failure(e2e_fixture: End2EndFixture) -> None:
    def after_feature(context: Context, feature: Feature) -> None:
        from grizzly.locust import on_master
        if on_master(context):
            return

        grizzly = cast(GrizzlyContext, context.grizzly)

        stats = grizzly.state.locust.environment.stats

        expectations = [
            # failure exception is StopUser, abort user/scenario when there's a failure
            ('001 stop user', 'SCEN', 1, 1,),
            ('001 stop user', 'TSTD', 0, 1,),
            ('001 stop-get1', 'GET', 0, 1,),
            ('001 stop-get2', 'GET', 1, 1,),
            # failure exception is RestartScenario, do not run steps after the failing step, restart from task 0
            ('002 restart scenario', 'SCEN', 1, 2,),
            ('002 restart scenario', 'TSTD', 0, 2,),
            ('002 restart-get1', 'GET', 0, 2,),
            ('002 restart-get2', 'GET', 1, 2,),
            ('002 restart-get3', 'GET', 0, 1,),
            # failure exception is None, just continue
            ('003 default', 'SCEN', 0, 2,),
            ('003 default', 'TSTD', 0, 2,),
            ('003 default-get1', 'GET', 0, 2,),
            ('003 default-get2', 'GET', 1, 2,),
            ('003 default-get3', 'GET', 0, 2,),
        ]

        for name, method, expected_num_failures, expected_num_requests in expectations:
            stat = stats.get(name, method)
            assert stat.num_failures == expected_num_failures, f'{stat.method}:{stat.name}.num_failures: {stat.num_failures} != {expected_num_failures}'
            assert stat.num_requests == expected_num_requests, f'{stat.method}:{stat.name}.num_requests: {stat.num_requests} != {expected_num_requests}'

    e2e_fixture.add_after_feature(after_feature)

    if e2e_fixture._distributed:
        start_webserver_step = f'Then start webserver on master port "{e2e_fixture.webserver.port}"\n'
    else:
        start_webserver_step = ''

    feature_file = e2e_fixture.create_feature(dedent(f'''Feature: test failure
    Background: common configuration
        Given "3" users
        And spawn rate is "3" user per second
        {start_webserver_step}
    Scenario: stop user
        Given a user of type "RestApi" load testing "http://{e2e_fixture.host}"
        And repeat for "2" iterations
        And stop user on failure
        Then get request with name "stop-get1" from endpoint "/api/echo"
        Then get request with name "stop-get2" from endpoint "/api/until/hello?nth=2&wrong=foobar&right=world | content_type=json"
        When response payload "$.hello" is not "world" fail request
        Then get request with name "stop-get3" from endpoint "/api/echo"

    Scenario: restart scenario
        Given a user of type "RestApi" load testing "http://{e2e_fixture.host}"
        And repeat for "2" iterations
        And restart scenario on failure
        Then get request with name "restart-get1" from endpoint "/api/echo"
        Then get request with name "restart-get2" from endpoint "/api/until/hello?nth=2&wrong=foobar&right=world | content_type=json"
        When response payload "$.hello" is not "world" fail request
        Then get request with name "restart-get3" from endpoint "/api/echo"

    Scenario: default
        Given a user of type "RestApi" load testing "http://{e2e_fixture.host}"
        And repeat for "2" iterations
        Then get request with name "default-get1" from endpoint "/api/echo"
        Then get request with name "default-get2" from endpoint "/api/until/hello?nth=2&wrong=foobar&right=world | content_type=json"
        When response payload "$.hello" is not "world" fail request
        Then get request with name "default-get3" from endpoint "/api/echo"
    '''))

    rc, output = e2e_fixture.execute(feature_file)

    assert rc == 1
    assert "HOOK-ERROR in after_feature: RuntimeError: locust test failed" in ''.join(output)

    log_files = list((e2e_fixture.root / 'features' / 'logs').glob('*.log'))

    assert len(log_files) == 3
