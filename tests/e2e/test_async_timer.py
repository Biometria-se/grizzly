"""End-to-end tests of grizzly.tasks.async_timer."""
from __future__ import annotations

from textwrap import dedent
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from tests.fixtures import End2EndFixture


def test_e2e_async_timers(e2e_fixture: End2EndFixture) -> None:
    feature_file = e2e_fixture.create_feature(dedent("""Feature: test async timers
    Background: common configuration
        Given "2" users
        And spawn rate is "2" user per second
    Scenario: first
        Given a user of type "Dummy" load testing "dummy://test"
        And repeat for "1" iterations
        Then start document timer with name "timer-1" for id "foobar-1" and version "1"
        Then start document timer with name "timer-1" for id "foobar-1" and version "2"
        Then start document timer with name "timer-2" for id "foobar-2" and version "1"
        Then start document timer with name "timer-2" for id "foobar-2" and version "2"
    Scenario: second
        Given a user of type "Dummy" load testing "dummy://test"
        And repeat for "1" iterations

        Then wait for "3.5" seconds

        Then stop document timer for id "foobar-1" and version "1"
        Then stop document timer with name "timer-2" for id "foobar-2" and version "2"
    """))

    rc, output = e2e_fixture.execute(feature_file)

    assert rc == 0

    result = ''.join(output)

    try:
        assert "The following asynchronous timers has not been stopped:" in result
        assert "- timer-1 (1):" in result
        assert "* foobar-1 (version 2):" in result
        assert "- timer-2 (1):" in result
        assert "* foobar-2 (version 1):" in result

        assert result.count('DOC      timer-1') >= 2
        assert result.count('DOC      timer-2') >= 2
    except:
        print(result)
        raise


