from textwrap import dedent

from tests.fixtures import End2EndFixture


def test_e2e_variables(e2e_fixture: End2EndFixture) -> None:
    feature_file = e2e_fixture.create_feature(dedent('''Feature: variables
    Background: common configuration
        Given "2" users
        And spawn rate is "2" users per second
        And value for variable "background_variable" is "foobar"
    Scenario: Scenario 1
        Given a user of type "Dummy" load testing "null"
        And repeat for "1" iteration
        And value for variable "scenario_1" is "{{ background_variable }}"

        Then log message "scenario_1={{ scenario_1 }}"
        Then log message "background_variable={{ background_variable }}"
    Scenario: Scenario 2
        Given a user of type "Dummy" load testing "null"
        And repeat for "1" iteration
        And value for variable "scenario_2" is "{{ background_variable }}"

        Then log message "scenario_2={{ scenario_2 }}"
        Then log message "background_variable={{ background_variable }}"
    '''))

    rc, output = e2e_fixture.execute(feature_file)

    result = '\n'.join(output)

    assert rc == 0

    assert result.count('scenario_1=foobar') == 1
    assert result.count('scenario_2=foobar') == 1
    assert result.count('background_variable=foobar') == 2
