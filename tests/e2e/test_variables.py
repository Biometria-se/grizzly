"""End-to-end test of grizzly variables."""
from __future__ import annotations

from textwrap import dedent
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from tests.fixtures import End2EndFixture


def test_e2e_variables(e2e_fixture: End2EndFixture) -> None:
    feature_file = e2e_fixture.create_feature(dedent("""Feature: variables
    Background: common configuration
        Given spawn rate is "2" users per second
        And value for variable "background_variable" is "foobar"
        And value for variable "AtomicIntegerIncrementer.test" is "10"
    Scenario: Scenario 1
        Given "2" users of type "Dummy" load testing "null"
        And repeat for "2" iteration
        And wait "0.0..0.5" seconds between tasks
        And value for variable "scenario_1" is "{{ background_variable }}"
        And value for variable "AtomicRandomString.scenario" is "AA%s | upper=True, count=10"

        Then log message "scenario_1={{ scenario_1 }}"
        Then log message "background_variable={{ background_variable }}"
        Then log message "Scenario 1::AtomicIntegerIncrementer.test={{ AtomicIntegerIncrementer.test }}"
        Then log message "Scenario 1::AtomicRandomString.scenario={{ AtomicRandomString.scenario }}"
    Scenario: Scenario 2
        Given "2" users of type "Dummy" load testing "null"
        And repeat for "2" iteration
        And wait "0.0..0.5" seconds between tasks
        And value for variable "scenario_2" is "{{ background_variable }}"
        And value for variable "AtomicRandomString.scenario" is "BB%s | upper=True, count=5"

        Then log message "scenario_2={{ scenario_2 }}"
        Then log message "background_variable={{ background_variable }}"
        Then log message "Scenario 2::AtomicIntegerIncrementer.test={{ AtomicIntegerIncrementer.test }}"
        Then log message "Scenario 2::AtomicRandomString.scenario={{ AtomicRandomString.scenario }}"
    """))

    rc, output = e2e_fixture.execute(feature_file)

    result = ''.join(output)

    assert rc == 0

    print(result)

    assert result.count('scenario_1=foobar') == 2
    assert result.count('scenario_2=foobar') == 2
    assert result.count('background_variable=foobar') == 4
    assert result.count('Scenario 1::AtomicIntegerIncrementer.test=10') == 1
    assert result.count('Scenario 1::AtomicIntegerIncrementer.test=11') == 1
    assert result.count('Scenario 2::AtomicIntegerIncrementer.test=10') == 1
    assert result.count('Scenario 2::AtomicIntegerIncrementer.test=11') == 1
    assert result.count('Scenario 1::AtomicRandomString.scenario=AA') == 2
    assert result.count('Scenario 2::AtomicRandomString.scenario=BB') == 2
