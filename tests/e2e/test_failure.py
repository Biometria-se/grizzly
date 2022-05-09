from textwrap import dedent
from typing import cast

from behave.runner import Context
from behave.model import Feature
from grizzly.context import GrizzlyContext

from ..fixtures import BehaveContextFixture, Webserver


def test_e2e_failure(behave_context_fixture: BehaveContextFixture, webserver: Webserver) -> None:
    def after_feature(context: Context, feature: Feature) -> None:
        grizzly = cast(GrizzlyContext, context.grizzly)

        stats = grizzly.state.environment.stats

        # failure exception is StopUser, abort user/scenario when there's a failure
        stat = stats.get('001 stop-get1', 'GET')
        assert stat.num_failures == 0, f'{stat.name}.num_failures: {stats.num_failures} != 0'
        assert stat.num_requests == 1, f'{stat.name}.num_requests: {stats.num_requests} != 0'

        stat = stats.get('001 stop-get2', 'GET')
        assert stat.num_failures == 1, f'{stat.name}.num_failures: {stats.num_failures} != 0'
        assert stat.num_requests == 1

        stat = stats.get('001 stop-get3', 'GET')
        assert stat.num_failures == 0, f'{stat.name}.num_failures: {stats.num_failures} != 0'
        assert stat.num_requests == 0

        # failure exception is RestartScenario, do not run steps after the failing step, restart from task 0
        stat = stats.get('002 restart-get1', 'GET')
        assert stat.num_failures == 0, f'{stat.name}.num_failures: {stats.num_failures} != 0'
        assert stat.num_requests == 2

        stat = stats.get('002 restart-get2', 'GET')
        assert stat.num_failures == 1, f'{stat.name}.num_failures: {stats.num_failures} != 0'
        assert stat.num_requests == 2

        stat = stats.get('002 restart-get3', 'GET')
        assert stat.num_failures == 0, f'{stat.name}.num_failures: {stats.num_failures} != 0'
        assert stat.num_requests == 1

        # failure exception is None, just continue
        stat = stats.get('003 default-get1', 'GET')
        assert stat.num_failures == 0, f'{stat.name}.num_failures: {stats.num_failures} != 0'
        assert stat.num_requests == 2

        stat = stats.get('003 default-get2', 'GET')
        assert stat.num_failures == 1, f'{stat.name}.num_failures: {stats.num_failures} != 0'
        assert stat.num_requests == 2

        stat = stats.get('003 default-get3', 'GET')
        assert stat.num_failures == 0, f'{stat.name}.num_failures: {stats.num_failures} != 0'
        assert stat.num_requests == 2

    behave_context_fixture.add_after_feature(after_feature)

    feature_file = behave_context_fixture.create_feature(dedent(f'''Feature: test failure
    Background: common configuration
        Given "3" users
        And spawn rate is "3" user per second

    Scenario: stop user
        Given a user of type "RestApi" load testing "http://localhost:{webserver.port}"
        And repeat for "2" iterations
        And stop user on failure
        Then get request with name "stop-get1" from endpoint "/api/echo"
        Then get request with name "stop-get2" from endpoint "/api/until/hello?nth=2&wrong=foobar&right=world | content_type=json"
        When response payload "$.hello" is not "world" fail scenario
        Then get request with name "stop-get3" from endpoint "/api/echo"

    Scenario: restart scenario
        Given a user of type "RestApi" load testing "http://localhost:{webserver.port}"
        And repeat for "2" iterations
        And restart scenario on failure
        Then get request with name "restart-get1" from endpoint "/api/echo"
        Then get request with name "restart-get2" from endpoint "/api/until/hello?nth=2&wrong=foobar&right=world | content_type=json"
        When response payload "$.hello" is not "world" fail scenario
        Then get request with name "restart-get3" from endpoint "/api/echo"

    Scenario: default
        Given a user of type "RestApi" load testing "http://localhost:{webserver.port}"
        And repeat for "2" iterations
        Then get request with name "default-get1" from endpoint "/api/echo"
        Then get request with name "default-get2" from endpoint "/api/until/hello?nth=2&wrong=foobar&right=world | content_type=json"
        When response payload "$.hello" is not "world" fail scenario
        Then get request with name "default-get3" from endpoint "/api/echo"
    '''))

    rc, _ = behave_context_fixture.execute(feature_file)

    assert rc == 0
