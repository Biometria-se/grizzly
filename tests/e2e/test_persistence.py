from textwrap import dedent
from os import path

from grizzly.types.behave import Context, Feature

from tests.fixtures import End2EndFixture


def test_e2e_persistence(e2e_fixture: End2EndFixture) -> None:
    def after_feature(context: Context, feature: Feature) -> None:
        from pathlib import Path
        from json import loads as jsonloads
        from grizzly.locust import on_worker

        if on_worker(context):
            return

        persist_file = Path(context.config.base_dir) / 'persistent' / f'{Path(feature.filename).stem}.json'

        assert persist_file.exists()

        persistance = jsonloads(persist_file.read_text())

        if feature.scenarios[0].name.strip() == 'run=1':
            expected_value = 3
        elif feature.scenarios[0].name.strip() == 'run=2':
            expected_value = 5
        else:
            raise AssertionError(f'unhandled scenario name "{feature.scenarios[0].name}"')

        assert persistance == {
            'AtomicIntegerIncrementer.persistent': f'{expected_value} | step=1, persist=True',
            'grizzly::keystore': {'foobar': ['hello', '{{ AtomicIntegerIncrementer.persistent }}']}
        }

    e2e_fixture.add_after_feature(after_feature)

    if e2e_fixture._distributed:
        start_webserver_step = f'Then start webserver on master port "{e2e_fixture.webserver.port}"\n'
    else:
        start_webserver_step = ''

    feature_file = e2e_fixture.create_feature(dedent(f'''Feature: test persistence
    Background: common configuration
        Given "1" users
        And spawn rate is "1" user per second
        {start_webserver_step}
    Scenario: run=1
        Given a user of type "RestApi" load testing "http://{e2e_fixture.host}"
        And repeat for "2" iterations
        And value for variable "key_holder" is "none"
        And value for variable "AtomicIntegerIncrementer.persistent" is "1 | step=1, persist=True"
        Then get "foobar" from keystore and save in variable "key_holder", with default value "['hello', '{{{{ AtomicIntegerIncrementer.persistent }}}}']"
        Then get request with name "get1" from endpoint "/api/echo?persistent={{{{ AtomicIntegerIncrementer.persistent }}}}"
        Then log message "persistent={{{{ AtomicIntegerIncrementer.persistent }}}}"
        Then log message "foobar={{{{ key_holder }}}}"
    '''))

    rc, output = e2e_fixture.execute(feature_file)

    result = ''.join(output)

    assert rc == 0
    assert 'persistent=1' in result
    assert "foobar=['hello', '1']" in result
    assert 'persistent=2' in result
    assert "foobar=['hello', '2']" in result

    with open(path.join(e2e_fixture.root, feature_file), 'r+') as fd:
        contents = fd.read()
        fd.truncate(0)

    contents = contents.replace('Scenario: run=1', 'Scenario: run=2')

    with open(path.join(e2e_fixture.root, feature_file), 'w') as fd:
        fd.write(contents)

    rc, output = e2e_fixture.execute(feature_file)

    result = ''.join(output)

    assert rc == 0
    assert 'persistent=3' in result
    assert "foobar=['hello', '3']" in result
    assert 'persistent=4' in result
    assert "foobar=['hello', '4']" in result
