"""Tests for grizzly-cli * run."""

from __future__ import annotations

import sys
from os import pathsep
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import TYPE_CHECKING

import pytest
import yaml

from test_cli.helpers import rm_rf, run_command

if TYPE_CHECKING:
    from test_cli.fixtures import End2EndFixture


def prepare_example_project(e2e_fixture: End2EndFixture) -> Path:
    example_root = e2e_fixture.root / 'grizzly-example'

    example_root.mkdir()

    rc, output = run_command(
        [
            'git',
            'init',
        ],
        cwd=example_root,
    )

    try:
        assert rc == 0
    except AssertionError:
        print(''.join(output))
        raise

    rc, output = run_command(
        [
            'git',
            'remote',
            'add',
            '-f',
            'origin',
            'https://github.com/Biometria-se/grizzly.git',
        ],
        cwd=example_root,
    )

    try:
        assert rc == 0
    except AssertionError:
        print(''.join(output))
        raise

    rc, output = run_command(
        [
            'git',
            'sparse-checkout',
            'init',
        ],
        cwd=example_root,
    )

    try:
        assert rc == 0
    except AssertionError:
        print(''.join(output))
        raise

    rc, output = run_command(
        [
            'git',
            'sparse-checkout',
            'set',
            'example',
        ],
        cwd=example_root,
    )

    try:
        assert rc == 0
    except AssertionError:
        print(''.join(output))
        raise

    rc, output = run_command(
        [
            'git',
            'pull',
            'origin',
            'main',
        ],
        cwd=example_root,
    )

    try:
        assert rc == 0
    except AssertionError:
        print(''.join(output))
        raise

    rm_rf(example_root / '.git')

    return example_root / 'example'


def validate_result(rc: int, result: str, example_root: Path) -> None:
    assert rc == 0
    assert 'ERROR' not in result
    assert 'WARNING' not in result
    assert '1 feature passed, 0 failed, 0 skipped' in result
    assert '3 scenarios passed, 0 failed, 0 skipped' in result
    assert 'steps passed, 0 failed, 0 skipped' in result

    assert 'ident   iter  status   description' in result
    assert '001      2/2  passed   dog facts api' in result
    assert '002      1/1  passed   cat facts api' in result
    assert '003      1/1  passed   book api' in result
    assert '------|-----|--------|---------------|' in result

    assert 'executing custom.User.request for 002 get-cat-facts and /facts?limit=' in result

    assert 'sending "client_server" from CLIENT' in result
    assert 'received from CLIENT' in result
    assert "AtomicCustomVariable.foobar='foobar'" in result

    assert 'compose.yaml: `version` is obsolete' not in result

    log_file_result = (example_root / 'test_run.log').read_text()

    # problems with a locust DEBUG log message containing ERROR in the message on macos-latest
    if sys.platform == 'darwin':
        output = [line for line in log_file_result.split('\n') if 'ERROR' not in line and 'DEBUG' not in line]
        log_file_result = '\n'.join(output)

    if sys.version_info >= (3, 12):
        result = result.replace('\r', '\n')

    assert log_file_result == result


def install_dependencies(e2e_fixture: End2EndFixture, example_root: Path) -> None:
    if e2e_fixture._distributed:
        command = ['grizzly-cli', 'dist', '--project-name', e2e_fixture.root.name, 'build', '--no-cache']
        rc, output = run_command(
            command,
            cwd=example_root,
            env=e2e_fixture._env,
        )
        try:
            assert rc == 0
        except AssertionError:
            with e2e_fixture.log_file.open('a+') as fd:
                fd.write(''.join(output))
            raise
    else:
        command = ['uv', 'sync', '--active', '--locked', '--package', 'grizzly-loadtester']
        repo_root_path = (Path(__file__).parent / '..' / '..' / '..' / '..').resolve()

        rc, output = run_command(
            command,
            cwd=repo_root_path,
            env=e2e_fixture._env,
        )

        try:
            assert rc == 0
        except AssertionError:
            with e2e_fixture.log_file.open('a+') as fd:
                fd.write(''.join(output))
            raise


def test_e2e_run_example(e2e_fixture: End2EndFixture) -> None:
    if sys.platform == 'win32' and e2e_fixture._distributed:
        pytest.skip('windows github runners do not support running linux containers')

    result: str | None = None

    try:
        example_root = prepare_example_project(e2e_fixture)

        with (example_root / 'features' / 'steps' / 'steps.py').open('a') as fd:
            fd.write(e2e_fixture.start_webserver_step_impl(e2e_fixture.webserver_port))

        e2e_fixture.inject_webserver_module(example_root)

        with (example_root / 'environments' / 'example.yaml').open('r') as env_yaml_file:
            env_conf = yaml.full_load(env_yaml_file)

            for name in ['dog', 'cat', 'book']:
                env_conf['configuration']['facts'][name]['host'] = f'http://{e2e_fixture.host}'

        feature_file = Path.joinpath(Path('features'), 'example.feature')
        feature_file_path = example_root / 'features' / 'example.feature'
        feature_file_contents = feature_file_path.read_text().split('\n')

        requirements_file = example_root / 'requirements.txt'
        # will used what's installed in VIRTUAL_ENV already
        requirements_file.write_text('grizzly-loadtester\ngrizzly-loadtester-common\n')

        install_dependencies(e2e_fixture, example_root)

        index = feature_file_contents.index('  Scenario: dog facts api')
        # should go last in "Background"-section
        feature_file_contents.insert(index - 1, f'    Then start webserver on master port "{e2e_fixture.webserver_port}"')

        with feature_file_path.open('w') as fd:
            fd.truncate(0)
            fd.write('\n'.join(feature_file_contents))

        with NamedTemporaryFile(delete=False, suffix='.yaml', dir=f'{example_root}/environments') as env_conf_file:
            env_conf_file.write(yaml.dump(env_conf, Dumper=yaml.Dumper).encode())
            env_conf_file.flush()

            rc, output = e2e_fixture.execute(
                feature_file,
                env_conf_file.name.replace(f'{example_root.as_posix()}{pathsep}', ''),
                cwd=example_root,
                arguments=['-l', 'test_run.log'],
            )

            # problems with a locust DEBUG log message containing ERROR in the message on macos-latest
            if sys.platform == 'darwin':
                output = [line for line in output if 'ERROR' not in line and 'DEBUG' not in line]

            result = ''.join(output)

            validate_result(rc, result, example_root)
    except:
        if result is not None:
            with e2e_fixture.log_file.open('a+') as fd:
                fd.write(''.join(output))
        raise
