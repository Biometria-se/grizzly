from textwrap import dedent

from grizzly.types.behave import Context, Feature

from tests.fixtures import End2EndFixture


def test_e2e_keystore(e2e_fixture: End2EndFixture) -> None:
    def after_feature(context: Context, feature: Feature) -> None:
        from pathlib import Path
        from json import loads as jsonloads
        from grizzly.locust import on_worker

        if on_worker(context):
            return

        persist_file = Path(context.config.base_dir) / 'persistent' / f'{Path(feature.filename).stem}.json'

        assert persist_file.exists()

        persistance = jsonloads(persist_file.read_text())

        assert persistance == {
            'grizzly::keystore': {
                'foobar::set': 'hello',
                'foobar::get::default': {'hello': 'world'},
            }
        }

    e2e_fixture.add_after_feature(after_feature)

    if e2e_fixture._distributed:
        start_webserver_step = f'Then start webserver on master port "{e2e_fixture.webserver.port}"\n'
    else:
        start_webserver_step = ''

    feature_file = e2e_fixture.create_feature(dedent(f'''Feature: test persistence
    Background: common configuration
        Given "2" users
        And spawn rate is "2" user per second
        {start_webserver_step}
    Scenario: first
        Given a user of type "Dummy" load testing "dummy://test"
        And repeat for "1" iterations
        And value for variable "key_holder_1" is "none"
        And wait "0" seconds between tasks
        Then set "foobar::set" in keystore with value "'hello'"
        Then get "foobar::get::default" from keystore and save in variable "key_holder_1", with default value "{{'hello': 'world'}}"
        Then log message "key_holder_1={{{{ key_holder_1 }}}}"
    Scenario: second
        Given a user of type "Dummy" load testing "dummy://test"
        And repeat for "1" iterations
        And value for variable "key_holder_2" is "none"
        And value for variable "key_holder_3" is "none"
        And wait "0.9" seconds between tasks
        Then get "foobar::set" from keystore and save in variable "key_holder_2"
        Then get "foobar::get::default" from keystore and save in variable "key_holder_3"
        Then log message "key_holder_2={{{{ key_holder_2 }}}}, key_holder_3={{{{ key_holder_3 }}}}"
    '''))

    rc, output = e2e_fixture.execute(feature_file)

    assert rc == 0

    result = ''.join(output)

    assert "key_holder_1={'hello': 'world'}" in result
    assert "key_holder_2=hello, key_holder_3={'hello': 'world'}" in result
