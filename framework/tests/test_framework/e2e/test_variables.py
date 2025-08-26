"""End-to-end test of grizzly variables."""

from __future__ import annotations

from textwrap import dedent
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.types.behave import Context, Feature

    from test_framework.fixtures import End2EndFixture


def test_e2e_variables(e2e_fixture: End2EndFixture) -> None:
    feature_file = e2e_fixture.create_feature(
        dedent("""Feature: variables
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
    """),
    )

    rc, output = e2e_fixture.execute(feature_file)

    result = ''.join(output)

    assert rc == 0

    assert result.count('scenario_1=foobar') == 2
    assert result.count('scenario_2=foobar') == 2
    assert result.count('background_variable=foobar') == 4
    assert result.count('Scenario 1::AtomicIntegerIncrementer.test=10') == 1
    assert result.count('Scenario 1::AtomicIntegerIncrementer.test=11') == 1
    assert result.count('Scenario 2::AtomicIntegerIncrementer.test=10') == 1
    assert result.count('Scenario 2::AtomicIntegerIncrementer.test=11') == 1
    assert result.count('Scenario 1::AtomicRandomString.scenario=AA') == 2
    assert result.count('Scenario 2::AtomicRandomString.scenario=BB') == 2


def test_e2e_variables_atomic_json_reader(e2e_fixture: End2EndFixture) -> None:
    def before_feature(context: Context, feature: Feature) -> None:  # noqa: ARG001
        import json
        from pathlib import Path

        context_root = Path(context.config.base_dir)
        test_json = context_root / 'requests' / 'test.json'
        test_json.parent.mkdir(exist_ok=True)

        with test_json.open('w') as fd:
            json.dump(
                [{'username': 'bob1', 'password': 'some-password'}, {'username': 'alice1', 'password': 'some-other-password'}, {'username': 'bob2', 'password': 'password'}],
                fd,
            )

    e2e_fixture.add_before_feature(before_feature)

    feature_file = e2e_fixture.test_steps(
        scenario=[
            'Given repeat for "3" iterations',
            'Given value for variable "AtomicJsonReader.test" is "test.json"',
            'Then log message "object={{ AtomicJsonReader.test | fromtestdata | stringify }}"',
            'Then log message "object.username={{ AtomicJsonReader.test.username }}"',
            'Then log message "object.password={{ AtomicJsonReader.test.password }}"',
        ],
    )

    rc, output = e2e_fixture.execute(feature_file)

    assert rc == 0

    result = ''.join(output)

    assert 'Exception ignored in' not in result
    assert 'object={"password": "some-password", "username": "bob1"}' in result
    assert 'object.username=bob1' in result
    assert 'object.password=some-password' in result

    assert 'object={"password": "some-other-password", "username": "alice1"}' in result
    assert 'object.username=alice1' in result
    assert 'object.password=some-other-password' in result

    assert 'object={"password": "password", "username": "bob2"}' in result
    assert 'object.username=bob2' in result
    assert 'object.password=password' in result
