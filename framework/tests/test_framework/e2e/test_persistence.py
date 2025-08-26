"""End-to-end tests of grizzly persistent variables."""

from __future__ import annotations

from textwrap import dedent
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.types.behave import Context, Feature

    from test_framework.fixtures import End2EndFixture


def test_e2e_persistence(e2e_fixture: End2EndFixture) -> None:
    def after_feature(context: Context, feature: Feature) -> None:
        from json import loads as jsonloads
        from pathlib import Path

        from grizzly.locust import on_worker

        if on_worker(context):
            return

        persist_file = Path(context.config.base_dir) / 'persistent' / f'{Path(feature.filename).stem}.json'

        assert persist_file.exists(), f'{persist_file.as_posix()} does not exist'

        persistance = jsonloads(persist_file.read_text())

        if feature.scenarios[0].name.strip() == 'run=1':
            expected_value = 3
        elif feature.scenarios[0].name.strip() == 'run=2':
            expected_value = 5
        else:
            msg = f'unhandled scenario name "{feature.scenarios[0].name}"'
            raise AssertionError(msg)

        assert persistance == {
            'IteratorScenario_001': {
                'AtomicIntegerIncrementer.persistent': f'{expected_value} | step=1, persist=True',
            },
        }

    e2e_fixture.add_after_feature(after_feature)

    start_webserver_step = f'Then start webserver on master port "{e2e_fixture.webserver.port}"\n' if e2e_fixture._distributed else ''

    feature_file = e2e_fixture.create_feature(
        dedent(f"""Feature: test persistence
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
    """),
    )

    rc, output = e2e_fixture.execute(feature_file)

    result = ''.join(output)

    try:
        assert rc == 0
        assert 'persistent=1' in result
        assert "foobar=['hello', '1']" in result
        assert 'persistent=2' in result
        assert "foobar=['hello', '2']" in result
    except AssertionError:
        print(result)
        for pfile in (e2e_fixture.root / 'persistent').glob('*.json'):
            print(f'!! {pfile.as_posix()}')
        raise

    actual_feature_file = e2e_fixture.root / feature_file
    contents = actual_feature_file.read_text()
    contents = contents.replace('Scenario: run=1', 'Scenario: run=2')
    actual_feature_file.write_text(contents)

    rc, output = e2e_fixture.execute(feature_file)

    result = ''.join(output)

    try:
        assert rc == 0
        assert 'Exception ignored in' not in result
        assert 'persistent=3' in result
        assert "foobar=['hello', '3']" in result
        assert 'persistent=4' in result
        assert "foobar=['hello', '4']" in result
    except AssertionError:
        print(result)
        raise
