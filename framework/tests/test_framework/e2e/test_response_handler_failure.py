"""End-to-end tests of grizzly.users.base.response_handler if one fails."""

from __future__ import annotations

from textwrap import dedent
from typing import TYPE_CHECKING

from test_framework.helpers import regex

if TYPE_CHECKING:  # pragma: no cover
    from test_framework.fixtures import End2EndFixture


def test_e2e_response_handler_failure(e2e_fixture: End2EndFixture) -> None:
    start_webserver_step = f'Then start webserver on master port "{e2e_fixture.webserver.port}"\n' if e2e_fixture._distributed else ''

    feature_file = e2e_fixture.create_feature(
        dedent(f"""Feature: test response handler failure
    Background: common configuration
        Given "1" users
        And spawn rate is "1" user per second
        {start_webserver_step}
    Scenario: first
        Given a user of type "RestApi" load testing "http://{e2e_fixture.host}"
        And repeat for "1" iterations
        And value for variable "matches1" is "None"
        And wait "0" seconds between tasks
        Then post request with name "post-payload" to endpoint "/api/echo | content_type=json"
          \"\"\"
          {{
              "hello": {{
                  "world": [
                    {{"value": "foo"}},
                    {{"value": "bar"}}
                  ]
              }}
          }}
          \"\"\"
        Then save response payload "$.hello.world[?value='foobar']" in variable "matches1"
        Then log message "matches1={{{{ matches1 }}}}"
    """),
    )

    # clean up logs directory first
    for log_file in list((e2e_fixture.root / 'features' / 'logs').glob('*.log')):
        log_file.unlink()

    rc, _ = e2e_fixture.execute(feature_file)

    assert rc == 1

    log_files = list((e2e_fixture.root / 'features' / 'logs').glob('*.log'))
    assert len(log_files) == 1

    log_file = log_files[0]
    assert log_file.name == regex(r'001-post-payload\.[0-9]{8}T[0-9]{12}\.log')

    log_file_contents = log_file.read_text()

    response_index = log_file_contents.index('/api/echo status=ERROR:')

    # check response
    assert """payload:
{"hello":{"world":[{"value":"foo"},{"value":"bar"}]}}""" in log_file_contents[response_index:]

    # check request
    assert """metadata:
{
  "Content-Type": "application/json",
  "x-grizzly-user": "RestApiUser_001"
}

payload:
{
    "hello": {
        "world": [
          {"value": "foo"},
          {"value": "bar"}
        ]
    }
}""" in log_file_contents[:response_index]
