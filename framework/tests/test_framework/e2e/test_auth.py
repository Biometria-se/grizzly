"""End-to-end tests of grizzly.auth.aad."""

from __future__ import annotations

from textwrap import dedent
from typing import TYPE_CHECKING, Any, cast

import pytest

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.context import GrizzlyContext
    from grizzly.types.behave import Context

    from test_framework.fixtures import End2EndFixture


def test_e2e_auth_user_token(e2e_fixture: End2EndFixture) -> None:
    if e2e_fixture._distributed:
        pytest.skip('telling the webserver what to expected auth-wise is not as simple when running dist, compare to running local')

    def before_feature(context: Context, *_args: Any, **_kwargs: Any) -> None:  # noqa: ARG001
        from grizzly_common.azure.aad import AzureAadCredential

        AzureAadCredential.provider_url_template = f'http://{e2e_fixture.host}/{{tenant}}{e2e_fixture.webserver.auth_provider_uri}'

    e2e_fixture.add_before_feature(before_feature)

    def after_feature(context: Context, *_args: Any, **_kwargs: Any) -> None:
        from grizzly.locust import on_master

        if on_master(context):
            return

        grizzly = cast('GrizzlyContext', context.grizzly)

        stats = grizzly.state.locust.environment.stats

        expectations = [
            ('001 AAD OAuth2 client token: dummy-client-id', 'AUTH', 0, 1),
            ('001 RestApi auth', 'SCEN', 0, 3),
            ('001 RestApi auth', 'TSTD', 0, 3),
            ('001 restapi-echo', 'GET', 0, 3),
            ('002 AAD OAuth2 client token: dummy-client-id', 'AUTH', 0, 1),
            ('002 HttpClientTask auth', 'SCEN', 0, 3),
            ('002 HttpClientTask auth', 'TSTD', 0, 3),
            ('002 httpclient-echo', 'CLTSK', 0, 3),
        ]

        for name, method, expected_num_failures, expected_num_requests in expectations:
            stat = stats.get(name, method)
            assert stat.num_failures == expected_num_failures, f'{stat.method}:{stat.name}.num_failures: {stat.num_failures} != {expected_num_failures}'
            assert stat.num_requests == expected_num_requests, f'{stat.method}:{stat.name}.num_requests: {stat.num_requests} != {expected_num_requests}'

    e2e_fixture.add_after_feature(after_feature)

    # inject expected properties
    e2e_fixture.webserver.auth = {
        'tenant': 'example.com',
        'client': {
            'id': 'dummy-client-id',
            'secret': 'dummy-client-secret',
        },
        'token': 'header.foobar.signature',
        'headers': {
            'x-subscription': 'foobar',
        },
    }

    def add_metadata() -> str:
        if e2e_fixture.webserver.auth is None:
            return ''

        steps: list[str] = []
        for key, value in e2e_fixture.webserver.auth.get('headers', {}).items():
            steps.append(f'And metadata "{key}" is "{value}"')

        return '\n'.join(steps)

    try:
        feature_file = e2e_fixture.create_feature(
            dedent(f"""Feature: test auth
        Background: common configuration
            Given "2" users
            And spawn rate is "2" users per second
        Scenario: RestApi auth
            Given a user of type "RestApi" load testing "http://{e2e_fixture.host}"
            And set context variable "auth.tenant" to "{e2e_fixture.webserver.auth['tenant']}"
            And set context variable "auth.client.id" to "{e2e_fixture.webserver.auth['client']['id']}"
            And set context variable "auth.client.secret" to "{e2e_fixture.webserver.auth['client']['secret']}"
            {add_metadata()}
            And repeat for "3" iterations
            Then get request with name "restapi-echo" from endpoint "/api/echo"

        Scenario: HttpClientTask auth
            Given a user of type "RestApi" load testing "http://{e2e_fixture.host}"
            And value for variable "foobar" is "none"
            And set context variable "{e2e_fixture.host}/auth.tenant" to "{e2e_fixture.webserver.auth['tenant']}"
            And set context variable "{e2e_fixture.host}/auth.client.id" to "{e2e_fixture.webserver.auth['client']['id']}"
            And set context variable "{e2e_fixture.host}/auth.client.secret" to "{e2e_fixture.webserver.auth['client']['secret']}"
            And repeat for "3" iterations
            Then get from "http://{e2e_fixture.host}/api/echo" with name "httpclient-echo" and save response payload in "foobar"
            {add_metadata()}
        """),
        )

        # patch so that we use our own webserver and not tries to test against real

        rc, _ = e2e_fixture.execute(feature_file)

        assert rc == 0
    finally:
        e2e_fixture.webserver.auth = None
