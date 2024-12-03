"""End-to-end tests of grizzly.tasks.async_timer."""
from __future__ import annotations

from textwrap import dedent
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from tests.fixtures import End2EndFixture


def test_e2e_async_timers(e2e_fixture: End2EndFixture) -> None:
    feature_file = e2e_fixture.create_feature(dedent("""Feature: test async timers
    Background: common configuration
        Given "3" users
        And spawn rate is "3" user per second
    Scenario: start
        Given a user of type "Dummy" load testing "dummy://test"
        And repeat for "5" iterations
        And value for variable "AtomicIntegerIncrementer.id" is "1 | step=1"
        Then start document timer with name "timer-1" for id "foobar-1" and version "{{ AtomicIntegerIncrementer.id }}"
        Then start document timer with name "timer-1" for id "foobar-1" and version "2{{ AtomicIntegerIncrementer.id }}"
        Then start document timer with name "timer-2" for id "foobar-2" and version "{{ AtomicIntegerIncrementer.id }}"
        Then start document timer with name "timer-2" for id "foobar-2" and version "2{{ AtomicIntegerIncrementer.id }}"

    Scenario: stop-1
        Given a user of type "Dummy" load testing "dummy://test"
        And repeat for "5" iterations
        And value for variable "AtomicIntegerIncrementer.id" is "1 | step=1"

        Then wait for "1.5" seconds

        Then stop document timer for id "foobar-1" and version "{{ AtomicIntegerIncrementer.id }}"

    Scenario: stop-2
        Given a user of type "Dummy" load testing "dummy://test"
        And repeat for "5" iterations
        And value for variable "AtomicIntegerIncrementer.id" is "1 | step=1"

        Then wait for "1.5" seconds

        Then stop document timer with name "timer-2" for id "foobar-2" and version "{{ AtomicIntegerIncrementer.id }}"
    """))

    rc, output = e2e_fixture.execute(feature_file)

    assert rc == 0

    result = ''.join(output)

    try:
        assert "The following asynchronous timers has not been stopped:" in result
        assert "- timer-1 (5):" in result
        assert "* foobar-1 (version 21):" in result
        assert "* foobar-1 (version 22):" in result
        assert "* foobar-1 (version 23):" in result
        assert "* foobar-1 (version 24):" in result
        assert "* foobar-1 (version 25):" in result
        assert "- timer-2 (5):" in result
        assert "* foobar-2 (version 21):" in result
        assert "* foobar-2 (version 22):" in result
        assert "* foobar-2 (version 23):" in result
        assert "* foobar-2 (version 24):" in result
        assert "* foobar-2 (version 25):" in result

        assert result.count('DOC      timer-1') >= 2
        assert result.count('DOC      timer-2') >= 2
    except:
        print(result)
        raise


