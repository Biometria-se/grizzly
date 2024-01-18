"""End-to-end tests of how scenarios are handled on failures."""
from __future__ import annotations

from textwrap import dedent
from typing import TYPE_CHECKING, Any, cast

from grizzly.context import GrizzlyContext

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.types.behave import Context
    from tests.fixtures import End2EndFixture


def test_e2e_scenario_failure_handling(e2e_fixture: End2EndFixture) -> None:
    def after_feature(context: Context, *_args: Any, **_kwargs: Any) -> None:
        from grizzly.locust import on_master
        if on_master(context):
            return

        grizzly = cast(GrizzlyContext, context.grizzly)

        stats = grizzly.state.locust.environment.stats

        expectations = [
            # failure exception is StopUser, abort user/scenario when there's a failure
            ('001 stop user', 'SCEN', 1, 1),
            ('001 stop user', 'TSTD', 0, 1),
            ('001 stop-get1', 'GET', 0, 1),
            ('001 stop-get2', 'GET', 1, 1),
            # failure exception is RestartScenario, do not run steps after the failing step, restart from task 0
            ('002 restart scenario', 'SCEN', 1, 2),
            ('002 restart scenario', 'TSTD', 0, 2),
            ('002 restart-get1', 'GET', 0, 2),
            ('002 restart-get2', 'GET', 1, 2),
            ('002 restart-get3', 'GET', 0, 1),
            # failure exception is None, just continue
            ('003 default', 'SCEN', 0, 2),
            ('003 default', 'TSTD', 0, 2),
            ('003 default-get1', 'GET', 0, 2),
            ('003 default-get2', 'GET', 1, 2),
            ('003 default-get3', 'GET', 0, 2),
        ]

        for name, method, expected_num_failures, expected_num_requests in expectations:
            stat = stats.get(name, method)
            assert stat.num_failures == expected_num_failures, f'{stat.method}:{stat.name}.num_failures: {stat.num_failures} != {expected_num_failures}'
            assert stat.num_requests == expected_num_requests, f'{stat.method}:{stat.name}.num_requests: {stat.num_requests} != {expected_num_requests}'

    e2e_fixture.add_after_feature(after_feature)

    start_webserver_step = f'Then start webserver on master port "{e2e_fixture.webserver.port}"\n' if e2e_fixture._distributed else ''

    feature_file = e2e_fixture.create_feature(dedent(f"""Feature: test scenario failure handling
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
    """))

    rc, output = e2e_fixture.execute(feature_file)

    assert rc == 1
    assert "HOOK-ERROR in after_feature: RuntimeError: locust test failed" in ''.join(output)

    log_files = list((e2e_fixture.root / 'features' / 'logs').glob('*.log'))

    assert len(log_files) == 3


def test_e2e_behave_failure(e2e_fixture: End2EndFixture) -> None:
    start_webserver_step = f'Then start webserver on master port "{e2e_fixture.webserver.port}"\n' if e2e_fixture._distributed else ''

    feature_file = e2e_fixture.create_feature(dedent(f"""Feature: test behave failure
    Background: common configuration
        Given "3" users
        And spawn rate is "3" user per second
        {start_webserver_step}
    Scenario: fails 1
        Given a user of type "RestApi" load testing "http://{e2e_fixture.host}"
        And repeat for "2" iterations
        And stop user on failure
        And value for variable "var" is "foobar"
        Then get request with name "{{{{ get1 }}}}" from endpoint "/api/echo"
        Then get request with name "get2" from endpoint "/api/until/hello?nth=2&wrong=foobar&right=world | content_type=json"
        Then save response payload "$.hello.world" in variable "var1"
        Then get request with name "get3" from endpoint "/api/echo"
        Then save response metadata "$.foobar" in variable "var"
        Then log message "var={{{{ var }}}}"

    Scenario: fails 2
        Given a user of type "RestApi" load testing "http://{e2e_fixture.host}"
        And repeat for "2" iterations
        And restart scenario on failure
        Then get request with name "get1" from endpoint "/api/echo"
        Then save response metadata "$.Content-Type" in variable "var2"
        Then get request with name "{{{{ get2 }}}}" from endpoint "/api/until/hello?nth=2&wrong=foobar&right=world | content_type=json"
        Then get request with name "get3" from endpoint "/api/echo | content_type=json"
        Then save response payload "$.bar" in variable "var"

    Scenario: fails 3
        Given a user of type "RestApi" load testing "http://{e2e_fixture.host}"
        And repeat for "2" iterations
        Then get request with name "get1" from endpoint "/api/echo"
        Then get request with name "get2" from endpoint "/api/until/hello?nth=2&wrong=foobar&right=world | content_type=json"
        When response payload "$.hello" is not "world" fail request
        Then get request with name "{{{{ get3 }}}}" from endpoint "/api/echo"
    """))

    rc, output = e2e_fixture.execute(feature_file)

    assert rc == 1

    result = ''.join(output)

    assert "HOOK-ERROR in after_feature: RuntimeError:" in result
    assert """Failure summary:
    Scenario: fails 1
        Then save response payload "$.hello.world" in variable "var1" # features/test_e2e_behave_failure.feature:13
            ! variable "var1" has not been declared
        Then save response metadata "$.foobar" in variable "var" # features/test_e2e_behave_failure.feature:15
            ! content type is not set for latest request

    Scenario: fails 2
        Then save response metadata "$.Content-Type" in variable "var2" # features/test_e2e_behave_failure.feature:23
            ! variable "var2" has not been declared

Started : """ in result
