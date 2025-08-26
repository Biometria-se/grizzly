"""End-to-end tests of grizzly.tasks.until."""

from __future__ import annotations

from textwrap import dedent
from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.context import GrizzlyContext
    from grizzly.types.behave import Context

    from test_framework.fixtures import End2EndFixture


def test_e2e_until(e2e_fixture: End2EndFixture) -> None:
    def after_feature(context: Context, *_args: Any, **_kwargs: Any) -> None:
        from grizzly.locust import on_worker

        if on_worker(context):
            return

        grizzly = cast('GrizzlyContext', context.grizzly)

        stats = grizzly.state.locust.environment.stats

        expectations = [
            ('001 RequestTask', 'SCEN', 0, 1),
            ('001 RequestTask', 'TSTD', 0, 1),
            ('001 request-task', 'GET', 1, 2),
            ('001 request-task, w=1.0s, r=2, em=1', 'UNTL', 0, 1),
            ('002 HttpClientTask', 'SCEN', 0, 1),
            ('002 HttpClientTask', 'TSTD', 0, 1),
            ('002 http-client-task', 'CLTSK', 1, 2),
            ('002 http-client-task, w=1.0s, r=3, em=1', 'UNTL', 0, 1),
        ]

        assert len(stats.errors) == 0, f'expected 0 logged errors, got {len(stats.errors)}'

        for name, method, expected_num_failures, expected_num_requests in expectations:
            stat = stats.get(name, method)
            assert stat.num_failures == expected_num_failures, f'{stat.method}:{stat.name}.num_failures: {stat.num_failures} != {expected_num_failures}'
            assert stat.num_requests == expected_num_requests, f'{stat.method}:{stat.name}.num_requests: {stat.num_requests} != {expected_num_requests}'

    e2e_fixture.add_after_feature(after_feature)

    start_webserver_step = f'Then start webserver on master port "{e2e_fixture.webserver.port}"\n\n' if e2e_fixture._distributed else ''

    env_conf: dict[str, dict[str, dict[str, str]]] = {
        'configuration': {
            'test': {
                'host': f'http://{e2e_fixture.host}',
            },
        },
    }

    feature_file = e2e_fixture.create_feature(
        dedent(f"""Feature: UntilRequestTask
    Background: common configuration
        Given "2" users
        And spawn rate is "2" user per second
        {start_webserver_step}
    Scenario: RequestTask
        Given a user of type "RestApi" load testing "http://{e2e_fixture.host}"
        And repeat for "1" iteration
        When any task fail stop user
        Then get request with name "request-task-reset" from endpoint "/api/until/reset"
        Then get request with name "request-task" from endpoint "/api/until/barbar?nth=2&wrong=foobar&right=world&as_array=true | content_type=json" until "$.`this`[?barbar="world"] | retries=2, expected_matches=1"

    Scenario: HttpClientTask
        Given a user of type "RestApi" load testing "http://{e2e_fixture.host}"
        And repeat for "1" iteration
        When any task fail stop user
        Then get from "http://$conf::test.host$/api/until/foofoo?nth=2&wrong=foobar&right=world&as_array=true | content_type=json" with name "http-client-task" until "$.`this`[?foofoo="world"] | retries=3, expected_matches=1"
    """),  # noqa: E501
    )

    rc, output = e2e_fixture.execute(feature_file, env_conf=env_conf)

    try:
        assert rc == 0
    except AssertionError:
        print(''.join(output))
        raise
