"""Tests for grizzly_cli:main."""

from __future__ import annotations

import sys
from argparse import ArgumentParser as CoreArgumentParser
from argparse import Namespace
from hashlib import sha1
from os import environ
from pathlib import Path
from typing import TYPE_CHECKING, cast

import pytest
from grizzly_cli.__main__ import _create_parser, _inject_additional_arguments_from_metadata, _parse_arguments, main

from test_cli.helpers import SOME, cwd, rm_rf

if TYPE_CHECKING:
    from _pytest.capture import CaptureFixture
    from _pytest.tmpdir import TempPathFactory
    from pytest_mock import MockerFixture


def test__create_parser() -> None:  # noqa: PLR0915
    parser = _create_parser()

    assert parser.prog == 'grizzly-cli'
    assert parser.description is not None
    assert 'pip install grizzly-loadtester-cli' in parser.description
    assert 'eval "$(grizzly-cli --bash-completion)"' in parser.description
    assert parser._subparsers is not None
    assert len(parser._subparsers._group_actions) == 1
    assert sorted([option_string for action in parser._actions for option_string in action.option_strings]) == sorted(
        [
            '-h',
            '--help',
            '--version',
            '--md-help',
            '--bash-completion',
        ]
    )
    assert sorted([action.dest for action in parser._actions if len(action.option_strings) == 0]) == ['command']
    subparser = parser._subparsers._group_actions[0]
    assert subparser is not None
    assert subparser.choices is not None
    assert len(cast('dict[str, CoreArgumentParser | None]', subparser.choices).keys()) == 5

    init_parser = cast('dict[str, CoreArgumentParser | None]', subparser.choices).get('init', None)
    assert init_parser is not None
    assert init_parser._subparsers is None
    assert getattr(init_parser, 'prog', None) == 'grizzly-cli init'
    assert sorted([option_string for action in init_parser._actions for option_string in action.option_strings]) == sorted(
        [
            '-h',
            '--help',
            '-y',
            '--yes',
            '--with-mq',
            '--grizzly-version',
        ]
    )

    auth_parser = cast('dict[str, CoreArgumentParser | None]', subparser.choices).get('auth', None)
    assert auth_parser is not None
    assert auth_parser._subparsers is None
    assert getattr(auth_parser, 'prog', None) == 'grizzly-cli auth'
    assert sorted([option_string for action in auth_parser._actions for option_string in action.option_strings]) == sorted(
        [
            '-h',
            '--help',
        ]
    )

    keyvault_parser = cast('dict[str, CoreArgumentParser | None]', subparser.choices).get('keyvault', None)
    assert keyvault_parser is not None
    print(keyvault_parser._subparsers)
    assert keyvault_parser._subparsers is not None
    assert getattr(keyvault_parser, 'prog', None) == 'grizzly-cli keyvault'
    assert sorted([option_string for action in keyvault_parser._actions for option_string in action.option_strings]) == sorted(
        [
            '--file',
            '-f',
            '-h',
            '--help',
            '--vault-name',
        ]
    )

    local_parser = cast('dict[str, CoreArgumentParser | None]', subparser.choices).get('local', None)
    assert local_parser is not None
    assert local_parser._subparsers is not None
    assert getattr(local_parser, 'prog', None) == 'grizzly-cli local'
    assert sorted([option_string for action in local_parser._actions for option_string in action.option_strings]) == sorted(
        [
            '-h',
            '--help',
        ]
    )
    assert len(local_parser._subparsers._group_actions) == 1
    local_subparser = local_parser._subparsers._group_actions[0]
    assert local_subparser is not None
    assert local_subparser.choices is not None
    assert list(cast('dict[str, CoreArgumentParser | None]', local_subparser.choices).keys()) == ['run']

    dist_parser = cast('dict[str, CoreArgumentParser | None]', subparser.choices).get('dist', None)
    assert dist_parser is not None
    assert dist_parser._subparsers is not None
    assert getattr(dist_parser, 'prog', None) == 'grizzly-cli dist'
    assert sorted([option_string for action in dist_parser._actions for option_string in action.option_strings]) == sorted(
        [
            '-h',
            '--help',
            '--force-build',
            '--build',
            '--validate-config',
            '--workers',
            '--id',
            '--limit-nofile',
            '--container-system',
            '--health-timeout',
            '--health-retries',
            '--health-interval',
            '--project-name',
            '--registry',
            '--tty',
            '--wait-for-worker',
        ]
    )
    assert sorted([action.dest for action in dist_parser._actions if len(action.option_strings) == 0]) == ['subcommand']
    assert len(dist_parser._subparsers._group_actions) == 1
    dist_subparser = dist_parser._subparsers._group_actions[0]
    assert dist_subparser is not None
    assert dist_subparser.choices is not None
    assert list(cast('dict[str, CoreArgumentParser | None]', dist_subparser.choices).keys()) == ['build', 'clean', 'run']

    dist_build_parser = cast('dict[str, CoreArgumentParser | None]', dist_subparser.choices).get('build', None)
    assert dist_build_parser is not None
    assert dist_build_parser._subparsers is None
    assert getattr(dist_build_parser, 'prog', None) == 'grizzly-cli dist build'
    assert sorted([option_string for action in dist_build_parser._actions for option_string in action.option_strings]) == sorted(
        [
            '-h',
            '--help',
            '--local-install',
            '--no-cache',
            '--registry',
            '--no-progress',
            '--verbose',
        ]
    )

    dist_clean_parser = cast('dict[str, CoreArgumentParser | None]', dist_subparser.choices).get('clean', None)
    assert dist_clean_parser is not None
    assert dist_clean_parser._subparsers is None
    assert getattr(dist_clean_parser, 'prog', None) == 'grizzly-cli dist clean'
    assert sorted([option_string for action in dist_clean_parser._actions for option_string in action.option_strings]) == sorted(
        [
            '-h',
            '--help',
            '--no-images',
            '--no-networks',
        ]
    )

    # grizzly-cli ... run
    for tested_parser, parent in [(local_parser, 'local'), (dist_parser, 'dist')]:
        assert tested_parser._subparsers is not None
        assert len(tested_parser._subparsers._group_actions) == 1
        subparser = tested_parser._subparsers._group_actions[0]
        run_parser = cast('dict[str, CoreArgumentParser | None]', subparser.choices).get('run', None)
        assert run_parser is not None
        assert getattr(run_parser, 'prog', None) == f'grizzly-cli {parent} run'
        assert sorted([option_string for action in run_parser._actions for option_string in action.option_strings]) == sorted(
            [
                '-h',
                '--help',
                '--verbose',
                '-T',
                '--testdata-variable',
                '-y',
                '--yes',
                '-e',
                '--environment-file',
                '--csv-prefix',
                '--csv-interval',
                '--csv-flush-interval',
                '-l',
                '--log-dir',
                '--log-file',
                '--dump',
                '--dry-run',
                '--profile',
            ]
        )
        assert sorted([action.dest for action in run_parser._actions if len(action.option_strings) == 0]) == ['file']


def test__parse_argument_version(capsys: CaptureFixture, mocker: MockerFixture, tmp_path_factory: TempPathFactory) -> None:  # noqa: PLR0915
    test_context = tmp_path_factory.mktemp('test_context')
    (test_context / 'test.feature').write_text('Feature:')

    import sys

    try:
        mocker.patch('grizzly_cli.EXECUTION_CONTEXT', test_context.as_posix())
        with cwd(test_context):
            sys.argv = ['grizzly-cli', '--version']

            expected_version = '0.0.0'
            expected_common_version = '1.2.3'

            mocker.patch('grizzly_cli.__main__.__version__', expected_version)
            mocker.patch('grizzly_cli.__main__.__common_version__', expected_common_version)

            with pytest.raises(SystemExit) as se:
                _parse_arguments()
            assert se.value.code == 0

            capture = capsys.readouterr()
            assert capture.err == ''
            assert capture.out == f'grizzly-cli {expected_version}\n└── grizzly-common {expected_common_version}\n'

            sys.argv = ['grizzly-cli', '--version', 'foo']

            with pytest.raises(SystemExit) as se:
                _parse_arguments()
            assert se.value.code == 2

            capture = capsys.readouterr()
            err = capture.err.split('\n')
            assert len(err) == 4
            assert err[0].startswith('usage: grizzly-cli')
            assert 'init,local,auth,dist,keyvault' in err[1]
            assert err[2] == ("grizzly-cli: error: argument --version: invalid choice: 'foo' (choose from 'all')") or (
                "grizzly-cli: error: argument --version: invalid choice: 'foo' (choose from all)"
            )
            assert err[3] == ''
            assert capture.out == ''

            requirements_file = test_context / 'requirements.txt'
            requirements_file.write_text('grizzly-loadtester==1.5.3\n')

            sys.argv = ['grizzly-cli', '--version', 'all']

            with pytest.raises(SystemExit) as se:
                _parse_arguments()
            assert se.value.code == 0

            capture = capsys.readouterr()
            assert capture.err == ''
            assert capture.out == f'grizzly-cli {expected_version}\n├── grizzly-common {expected_common_version}\n└── grizzly 1.5.3\n    └── locust 2.2.1\n'

            requirements_file.write_text('grizzly-loadtester[mq]==1.5.3\n')

            sys.argv = ['grizzly-cli', '--version', 'all']

            with pytest.raises(SystemExit) as se:
                _parse_arguments()
            assert se.value.code == 0
            capture = capsys.readouterr()
            assert capture.err == ''
            assert capture.out == f'grizzly-cli {expected_version}\n├── grizzly-common {expected_common_version}\n└── grizzly 1.5.3 ── extras: mq\n    └── locust 2.2.1\n'

            requirements_file.unlink()
            requirements_file.write_text('grizzly-loadtester[mq,dev]==1.5.3\n')

            sys.argv = ['grizzly-cli', '--version', 'all']

            with pytest.raises(SystemExit) as se:
                _parse_arguments()
            assert se.value.code == 0
            capture = capsys.readouterr()
            assert capture.err == ''
            assert capture.out == (f'grizzly-cli {expected_version}\n├── grizzly-common {expected_common_version}\n└── grizzly 1.5.3 ── extras: mq, dev\n    └── locust 2.2.1\n')

            def mocked_mkdtemp(prefix: str | None = '') -> str:
                return Path.joinpath(test_context, f'{prefix}test').as_posix()

            mocker.patch('grizzly_cli.utils.mkdtemp', mocked_mkdtemp)
            mocker.patch('grizzly_cli.utils.subprocess.check_call', return_value=0)
            mocker.patch('grizzly_cli.utils.subprocess.check_output', return_value='main\n')

            repo = 'git+https://git@github.com/biometria-se/grizzly.git@main#egg=grizzly-loadtester'
            repo_suffix = sha1(repo.encode('utf-8')).hexdigest()  # noqa: S324
            repo_dir = test_context / 'grizzly-cli-test' / f'grizzly-loadtester_{repo_suffix}'
            repo_dir.mkdir(parents=True)
            (repo_dir / 'pyproject.toml').touch()
            (repo_dir / 'setup.cfg').write_text('name = grizzly-loadtester\nversion = 0.0.0\n')
            (repo_dir / 'requirements.txt').write_text('locust==2.8.4  \\ \n')

            requirements_file.unlink()
            requirements_file.write_text(f'{repo}\n')

            sys.argv = ['grizzly-cli', '--version', 'all']

            with pytest.raises(SystemExit) as se:
                _parse_arguments()
            assert se.value.code == 0

            capture = capsys.readouterr()
            assert capture.err == ''
            assert capture.out == f'grizzly-cli {expected_version}\n├── grizzly-common {expected_common_version}\n└── grizzly 0.0.0\n    └── locust 2.8.4\n'

            repo = 'git+https://git@github.com/biometria-se/grizzly.git@main#egg=grizzly-loadtester[mq,dev]'
            repo_suffix = sha1(repo.encode('utf-8')).hexdigest()  # noqa: S324
            repo_dir = test_context / 'grizzly-cli-test' / f'grizzly-loadtester__mq_dev___{repo_suffix}'
            repo_dir.mkdir(parents=True)
            (repo_dir / 'pyproject.toml').touch()
            (repo_dir / 'setup.cfg').write_text('name = grizzly-loadtester\nversion = 0.0.0\n')
            (repo_dir / 'requirements.txt').write_text('locust==2.8.4  \\ \n')

            requirements_file.unlink()
            requirements_file.write_text(f'{repo}\n')

            sys.argv = ['grizzly-cli', '--version', 'all']

            with pytest.raises(SystemExit) as se:
                _parse_arguments()
            assert se.value.code == 0

            capture = capsys.readouterr()
            assert capture.err == ''
            assert capture.out == f'grizzly-cli {expected_version}\n├── grizzly-common {expected_common_version}\n└── grizzly 0.0.0 ── extras: mq, dev\n    └── locust 2.8.4\n'

            requirements_file.unlink()
            requirements_file.write_text('grizzly-loadtester==1.5.3\n')

            sys.argv = ['grizzly-cli', '--version', 'all']
            expected_version = '2.5.0'
            expected_common_version = '2.4.0'
            mocker.patch('grizzly_cli.__main__.__version__', expected_version)
            mocker.patch('grizzly_cli.__main__.__common_version__', expected_common_version)

            with pytest.raises(SystemExit) as se:
                _parse_arguments()
            assert se.value.code == 0

            capture = capsys.readouterr()
            assert capture.err == ''
            assert capture.out == f'grizzly-cli {expected_version}\n├── grizzly-common {expected_common_version}\n└── grizzly 1.5.3\n    └── locust 2.2.1\n'
    finally:
        rm_rf(test_context)


def test__parse_argument_local(capsys: CaptureFixture, mocker: MockerFixture, tmp_path_factory: TempPathFactory) -> None:  # noqa: PLR0915
    test_context = tmp_path_factory.mktemp('test_context')
    (test_context / 'test.feature').write_text('Feature:')

    import sys

    try:
        mocker.patch('grizzly_cli.EXECUTION_CONTEXT', test_context.as_posix())
        with cwd(test_context):
            sys.argv = ['grizzly-cli', 'local']

            with pytest.raises(SystemExit) as se:
                _parse_arguments()
            assert se.value.code == 2

            capture = capsys.readouterr()
            assert capture.out == ''
            assert capture.err == 'grizzly-cli: error: no subcommand for local specified\n'

            sys.argv = ['grizzly-cli', 'local', 'run', 'test.feature']
            mocker.patch('grizzly_cli.__main__.which', side_effect=[None])

            with pytest.raises(SystemExit) as se:
                _parse_arguments()
            assert se.value.code == 2

            capture = capsys.readouterr()
            assert capture.out == ''
            assert capture.err == 'grizzly-cli: error: "behave" not found in PATH, needed when running local mode\n'

            # csv logging
            sys.argv = ['grizzly-cli', 'local', 'run', '--csv-interval', '20', 'test.feature']
            mocker.patch('grizzly_cli.__main__.which', side_effect=['behave'])

            with pytest.raises(SystemExit) as se:
                _parse_arguments()
            assert se.value.code == 2

            capture = capsys.readouterr()
            assert capture.out == ''
            assert capture.err == 'grizzly-cli: error: --csv-interval can only be used in combination with --csv-prefix\n'

            sys.argv = ['grizzly-cli', 'local', 'run', '--csv-prefix', '--csv-interval', '20', '--csv-flush-interval', '60', 'test.feature']
            mocker.patch('grizzly_cli.__main__.which', side_effect=['behave'])

            parsed_args = _parse_arguments()

            assert getattr(parsed_args, 'csv_prefix', False)
            assert getattr(parsed_args, 'csv_interval', None) == 20
            assert getattr(parsed_args, 'csv_flush_interval', None) == 60

            sys.argv = ['grizzly-cli', 'local', 'run', '--csv-prefix', 'static csv prefix', 'test.feature']
            mocker.patch('grizzly_cli.__main__.which', side_effect=['behave'])

            parsed_args = _parse_arguments()

            assert getattr(parsed_args, 'csv_prefix', None) == 'static csv prefix'
            assert getattr(parsed_args, 'csv_interval', None) is None
            assert getattr(parsed_args, 'csv_flush_interval', None) is None
            # // csv logging

            # -T/--testdata-variable
            sys.argv = ['grizzly-cli', 'local', 'run', '-T', 'variable', 'test.feature']
            mocker.patch('grizzly_cli.__main__.which', side_effect=['behave'])

            with pytest.raises(SystemExit) as se:
                _parse_arguments()
            assert se.value.code == 2

            capture = capsys.readouterr()
            assert capture.out == ''
            assert capture.err == 'grizzly-cli: error: -T/--testdata-variable needs to be in the format NAME=VALUE\n'

            sys.argv = ['grizzly-cli', 'local', 'run', '-T', 'key=value', 'test.feature']
            mocker.patch('grizzly_cli.__main__.which', side_effect=['behave'])

            assert environ.get('TESTDATA_VARIABLE_key', None) is None  # noqa: SIM112

            arguments = _parse_arguments()
            assert arguments.command == 'local'
            assert arguments.subcommand == 'run'
            assert arguments.file == 'test.feature'

            assert environ.get('TESTDATA_VARIABLE_key', None) == 'value'  # noqa: SIM112
            # // -T/--testdata-variable
    finally:
        rm_rf(test_context)


def test__parse_argument_dist(capsys: CaptureFixture, mocker: MockerFixture, tmp_path_factory: TempPathFactory) -> None:  # noqa: PLR0915
    test_context = tmp_path_factory.mktemp('test_context')
    (test_context / 'test.feature').write_text('Feature:')

    import sys

    try:
        mocker.patch('grizzly_cli.EXECUTION_CONTEXT', test_context.as_posix())
        with cwd(test_context):
            sys.argv = ['grizzly-cli', 'dist', 'run', 'test.feature']

            mocker.patch('grizzly_cli.__main__.get_distributed_system', side_effect=[None])

            with pytest.raises(SystemExit) as se:
                _parse_arguments()
            assert se.value.code == 2

            capture = capsys.readouterr()
            assert capture.out == ''
            assert capture.err == 'grizzly-cli: error: cannot run distributed\n'

            mocker.patch('grizzly_cli.EXECUTION_CONTEXT', Path.cwd())
            mocker.patch('grizzly_cli.__main__.get_distributed_system', side_effect=['docker'])
            mocker.patch('grizzly_cli.distributed.do_build', side_effect=[0, 4, 0])

            sys.argv = ['grizzly-cli', 'dist', '--limit-nofile', '100', '--registry', 'ghcr.io/biometria-se', 'run', 'test.feature']
            (test_context / 'requirements.txt').write_text('grizzly-loadtester')
            mocker.patch('grizzly_cli.__main__.get_distributed_system', side_effect=['docker'])
            ask_yes_no = mocker.patch('grizzly_cli.__main__.ask_yes_no', autospec=True)

            arguments = _parse_arguments()
            capture = capsys.readouterr()
            assert arguments.limit_nofile == 100
            assert not arguments.yes
            assert arguments.registry == 'ghcr.io/biometria-se/'
            assert capture.out == '!! this will cause warning messages from locust later on\n'
            assert capture.err == ''
            assert ask_yes_no.call_count == 1
            args, _ = ask_yes_no.call_args_list[-1]
            assert args[0] == 'are you sure you know what you are doing?'

            sys.argv = ['grizzly-cli', 'dist', 'run', '--csv-flush-interval', '60', 'test.feature']
            mocker.patch('grizzly_cli.__main__.get_distributed_system', side_effect=['docker'])

            with pytest.raises(SystemExit) as se:
                _parse_arguments()
            assert se.value.code == 2

            capture = capsys.readouterr()
            assert capture.out == ''
            assert capture.err == 'grizzly-cli: error: --csv-flush-interval can only be used in combination with --csv-prefix\n'

            sys.argv = ['grizzly-cli', 'dist', 'build']
            mocker.patch('grizzly_cli.__main__.get_distributed_system', side_effect=['docker'] * 3)
            arguments = _parse_arguments()

            assert not arguments.no_cache
            assert not arguments.force_build
            assert arguments.build
            assert arguments.registry is None

            sys.argv = ['grizzly-cli', 'dist', 'build', '--no-cache', '--registry', 'registry.example.com/biometria-se']
            arguments = _parse_arguments()

            assert arguments.no_cache
            assert arguments.force_build
            assert not arguments.build
            assert arguments.registry == 'registry.example.com/biometria-se/'
    finally:
        rm_rf(test_context)


def test__parse_argument(capsys: CaptureFixture, mocker: MockerFixture, tmp_path_factory: TempPathFactory) -> None:
    test_context = tmp_path_factory.mktemp('test_context')
    (test_context / 'test.feature').write_text('Feature:')

    import sys

    try:
        mocker.patch('grizzly_cli.EXECUTION_CONTEXT', test_context.as_posix())
        with cwd(test_context):
            sys.argv = ['grizzly-cli']

            with pytest.raises(SystemExit) as se:
                _parse_arguments()

            assert se.value.code == 2
            capture = capsys.readouterr()
            assert capture.out == ''
            assert 'usage: grizzly-cli' in capture.err
            assert 'grizzly-cli: error: no command specified' in capture.err

            sys.argv = ['grizzly-cli', 'init', 'test-project']
            arguments = _parse_arguments()

            assert arguments.project == 'test-project'
            assert getattr(arguments, 'subcommand', None) is None
    finally:
        rm_rf(test_context)


def test__inject_additional_arguments_from_metadata(tmp_path_factory: TempPathFactory, capsys: CaptureFixture, mocker: MockerFixture) -> None:
    test_context = tmp_path_factory.mktemp('test_context')

    test_feature_file = test_context / 'test.feature'
    test_feature_file.touch()
    mocker.patch('grizzly_cli.__main__.get_distributed_system', return_value='docker')

    try:
        with cwd(test_context):
            test_feature_file.write_text('# grizzly-cli run --verbose\nFeature:\n')
            sys.argv = ['grizzly-cli', 'dist', 'run', 'test.feature']

            orig_args = _parse_arguments()
            assert not orig_args.verbose

            args = _inject_additional_arguments_from_metadata(orig_args)
            capture = capsys.readouterr()

            assert capture.err == capture.out == ''
            assert args.verbose

            test_feature_file.write_text('# grizzly-cli local --hello\nFeature:\n')
            sys.argv = ['grizzly-cli', 'dist', 'run', 'test.feature']

            orig_args = _parse_arguments()
            args = _inject_additional_arguments_from_metadata(orig_args)

            capture = capsys.readouterr()
            assert capture.err == ''
            assert capture.out == '?? ignoring local --hello\n'

            test_feature_file.write_text('Feature:\n')
            sys.argv = ['grizzly-cli', 'dist', 'run', 'test.feature']

            orig_args = _parse_arguments()
            args = _inject_additional_arguments_from_metadata(orig_args)

            assert args is orig_args

            capture = capsys.readouterr()
            assert capture.err == capture.out == ''

            test_feature_file.write_text('# grizzly-cli --health-timeout 100\nFeature:\n')
            sys.argv = ['grizzly-cli', 'dist', 'run', 'test.feature']

            orig_args = _parse_arguments()
            args = _inject_additional_arguments_from_metadata(orig_args)

            assert args.health_timeout == orig_args.health_timeout

            capture = capsys.readouterr()
            assert capture.err == ''
            assert capture.out == '?? ignoring --health-timeout 100\n'

            test_feature_file.write_text('# grizzly-cli dist --health-timeout 100 --health-retries 101\nFeature:\n# grizzly-cli dist --health-interval 5\n')
            sys.argv = ['grizzly-cli', 'dist', 'run', 'test.feature']

            orig_args = _parse_arguments()
            args = _inject_additional_arguments_from_metadata(orig_args)

            assert args.health_timeout == 100
            assert args.health_retries == 101
            assert args.health_interval == 5

            capture = capsys.readouterr()
            assert capture.err == capture.out == ''
    finally:
        rm_rf(test_context)


def test_main(mocker: MockerFixture, capsys: CaptureFixture) -> None:
    local_mock = mocker.patch('grizzly_cli.__main__.local', side_effect=[0])
    dist_mock = mocker.patch('grizzly_cli.__main__.distributed', side_effect=[1337, 1373])
    init_mock = mocker.patch('grizzly_cli.__main__.init', side_effect=[7331])
    inject_additional_arguments_from_metadata_mock = mocker.patch(
        'grizzly_cli.__main__._inject_additional_arguments_from_metadata',
        return_value=Namespace(command='dist', file='test.feature'),
    )
    mocker.patch(
        'grizzly_cli.__main__._parse_arguments',
        side_effect=[
            Namespace(command='local'),
            Namespace(command='dist'),
            Namespace(command='init'),
            Namespace(command='foobar'),
            KeyboardInterrupt,
            ValueError('hello there'),
            Namespace(command='dist', file='test.feature'),
        ],
    )

    assert main() == 0
    local_mock.assert_called_once_with(SOME(Namespace, command='local'))
    local_mock.reset_mock()
    dist_mock.assert_not_called()
    init_mock.assert_not_called()
    inject_additional_arguments_from_metadata_mock.assert_not_called()

    assert main() == 1337
    local_mock.assert_not_called()
    dist_mock.assert_called_once_with(SOME(Namespace, command='dist'))
    dist_mock.reset_mock()
    init_mock.assert_not_called()
    inject_additional_arguments_from_metadata_mock.assert_not_called()

    assert main() == 7331
    local_mock.assert_not_called()
    dist_mock.assert_not_called()
    init_mock.assert_called_once_with(SOME(Namespace, command='init'))
    init_mock.reset_mock()
    inject_additional_arguments_from_metadata_mock.assert_not_called()

    assert main() == 1

    capture = capsys.readouterr()
    assert capture.err == ''
    assert capture.out == '\nunknown command foobar\n\n!! aborted grizzly-cli\n'

    assert main() == 1

    capture = capsys.readouterr()
    assert capture.err == ''
    assert capture.out == '\n\n!! aborted grizzly-cli\n'

    assert main() == 1

    capture = capsys.readouterr()
    assert capture.err == ''
    assert capture.out == '\nhello there\n\n!! aborted grizzly-cli\n'

    assert main() == 1373
    capture = capsys.readouterr()
    local_mock.assert_not_called()
    dist_mock.assert_called_once_with(SOME(Namespace, command='dist', file='test.feature'))
    dist_mock.reset_mock()
    init_mock.assert_not_called()
    inject_additional_arguments_from_metadata_mock.assert_called_once_with(SOME(Namespace, command='dist', file='test.feature'))
