from typing import List, cast
from textwrap import dedent

import pytest

from grizzly.types.behave import Context, Feature
from grizzly.context import GrizzlyContext

from tests.fixtures import End2EndFixture


def test_e2e_auth(e2e_fixture: End2EndFixture) -> None:
    if e2e_fixture._distributed:
        pytest.skip('telling the webserver what to expected auth-wise is not as simple when running dist, compare to running local')

    def after_feature(context: Context, feature: Feature) -> None:
        from grizzly.locust import on_master
        if on_master(context):
            return

        grizzly = cast(GrizzlyContext, context.grizzly)

        stats = grizzly.state.locust.environment.stats

        expectations = [
            ('001 AAD OAuth2 user token v1.0', 'AUTH', 0, 1,),
            ('001 RestApi auth', 'SCEN', 0, 1,),
            ('001 RestApi auth', 'TSTD', 0, 1,),
            ('001 restapi-echo', 'GET', 0, 1,),
            ('002 AAD OAuth2 user token v1.0', 'AUTH', 0, 1,),
            ('002 HttpClientTask auth', 'SCEN', 0, 1,),
            ('002 HttpClientTask auth', 'TSTD', 0, 1,),
            ('002 httpclient-echo', 'CLTSK', 0, 1,),
        ]

        for name, method, expected_num_failures, expected_num_requests in expectations:
            stat = stats.get(name, method)
            assert stat.num_failures == expected_num_failures, f'{stat.method}:{stat.name}.num_failures: {stat.num_failures} != {expected_num_failures}'
            assert stat.num_requests == expected_num_requests, f'{stat.method}:{stat.name}.num_requests: {stat.num_requests} != {expected_num_requests}'

    e2e_fixture.add_after_feature(after_feature)

    # inject expected properties
    e2e_fixture.webserver.auth = {
        'client': {
            'id': 'dummy-client-id',
            'secret': 'dummy-client-secret',
        },
        'token': 'foobar',
        'headers': {
            'x-subscription': 'foobar',
        }
    }

    def add_metadata() -> str:
        if e2e_fixture.webserver.auth is None:
            return ''

        steps: List[str] = []
        for key, value in e2e_fixture.webserver.auth.get('headers', {}).items():
            steps.append(f'And metadata "{key}" is "{value}"')

        return '\n'.join(steps)

    try:
        feature_file = e2e_fixture.create_feature(dedent(f'''Feature: test auth
        Background: common configuration
            Given "2" users
            And spawn rate is "2" users per second
        Scenario: RestApi auth
            Given a user of type "RestApi" load testing "http://{e2e_fixture.host}"
            And set context variable "auth.provider" to "http://{e2e_fixture.host}{e2e_fixture.webserver.auth_provider_uri}"
            And set context variable "auth.client.id" to "{e2e_fixture.webserver.auth['client']['id']}"
            And set context variable "auth.client.secret" to "{e2e_fixture.webserver.auth['client']['secret']}"
            {add_metadata()}
            And repeat for "1" iterations
            Then get request with name "restapi-echo" from endpoint "/api/echo"

        Scenario: HttpClientTask auth
            Given a user of type "RestApi" load testing "http://{e2e_fixture.host}"
            And value for variable "foobar" is "none"
            And set context variable "{e2e_fixture.host}/auth.provider" to "http://{e2e_fixture.host}{e2e_fixture.webserver.auth_provider_uri}"
            And set context variable "{e2e_fixture.host}/auth.client.id" to "{e2e_fixture.webserver.auth['client']['id']}"
            And set context variable "{e2e_fixture.host}/auth.client.secret" to "{e2e_fixture.webserver.auth['client']['secret']}"
            And repeat for "1" iterations
            Then get "http://{e2e_fixture.host}/api/echo" with name "httpclient-echo" and save response payload in "foobar"
            {add_metadata()}
        '''))

        rc, _ = e2e_fixture.execute(feature_file)

        assert rc == 0
    finally:
        e2e_fixture.webserver.auth = None
