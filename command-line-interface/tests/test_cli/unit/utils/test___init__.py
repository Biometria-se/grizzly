"""Tests for grizzly_cli.utils."""

from __future__ import annotations

from argparse import Namespace
from contextlib import ExitStack
from importlib import reload
from json.decoder import JSONDecodeError
from pathlib import Path
from tempfile import gettempdir
from textwrap import dedent
from typing import TYPE_CHECKING, Any, Union
from unittest.mock import mock_open
from unittest.mock import patch as unittest_patch

import pytest
from grizzly_cli.utils import (
    ask_yes_no,
    distribution_of_users_per_scenario,
    find_metadata_notices,
    find_variable_names_in_questions,
    get_default_mtu,
    get_dependency_versions,
    get_distributed_system,
    list_images,
    parse_feature_file,
    requirements,
    run_command,
    setup_logging,
)

from test_cli.helpers import create_scenario, cwd, rm_rf

if TYPE_CHECKING:  # pragma: no cover
    from _pytest.capture import CaptureFixture
    from _pytest.tmpdir import TempPathFactory
    from pytest_mock import MockerFixture
    from requests_mock import Mocker as RequestsMocker


pytest_plugins = ['requests_mock']


def test_parse_feature_file(tmp_path_factory: TempPathFactory) -> None:
    test_context = tmp_path_factory.mktemp('test_context')
    test_context_root = str(test_context)
    feature_file = test_context / 'test.feature'
    feature_file.touch()
    feature_file.write_text(
        dedent("""
    Feature: test feature
        Background:
            Given a common test step
            When executed in every scenario
        Scenario: scenario-1
            Given a test step
            And another test step
        Scenario: scenario-2
            Given a second test step
            Then execute it
            When done, just stop
    """)
    )

    try:
        with cwd(test_context):
            import grizzly_cli

            reload(grizzly_cli)
            reload(grizzly_cli.utils)

            assert len(grizzly_cli.SCENARIOS) == 0

            parse_feature_file('test.feature')

            import grizzly_cli

            cached_scenarios = grizzly_cli.SCENARIOS.copy()
            assert len(grizzly_cli.SCENARIOS) == 2
            assert next(iter(grizzly_cli.SCENARIOS)).name == 'scenario-1'
            assert len(next(iter(grizzly_cli.SCENARIOS)).steps) == 2
            assert len(next(iter(grizzly_cli.SCENARIOS)).background_steps) == 2
            assert list(grizzly_cli.SCENARIOS)[1].name == 'scenario-2'
            assert len(list(grizzly_cli.SCENARIOS)[1].steps) == 3
            assert len(list(grizzly_cli.SCENARIOS)[1].background_steps) == 2

            parse_feature_file('test.feature')

            import grizzly_cli

            assert cached_scenarios == grizzly_cli.SCENARIOS
    finally:
        rm_rf(test_context_root)


def test_list_images(mocker: MockerFixture) -> None:
    check_output = mocker.patch(
        'grizzly_cli.utils.subprocess.check_output',
        side_effect=[
            (
                b'{"name": "mcr.microsoft.com/vscode/devcontainers/python", "tag": "0-3.10", "size": "1.16GB", "created": "2021-12-02 23:46:55 +0100 CET", "id": "a05f8cc8454b"}\n'
                b'{"name": "mcr.microsoft.com/vscode/devcontainers/python", "tag": "0-3.10-bullseye", "size": "1.16GB", "created": "2021-12-02 23:46:55 +0100 CET", "id": "a05f8cc8454b"}\n'  # noqa: E501
                b'{"name": "mcr.microsoft.com/vscode/devcontainers/python", "tag": "0-3.9", "size": "1.23GB", "created": "2021-12-02 23:27:50 +0100 CET", "id": "bfbce224d490"}\n'
                b'{"name": "mcr.microsoft.com/vscode/devcontainers/python", "tag": "0-3.8", "size": "1.23GB", "created": "2021-12-02 23:10:12 +0100 CET", "id": "8a04d9e5df14"}\n'
                b'{"name": "mcr.microsoft.com/vscode/devcontainers/base", "tag": "0-focal", "size": "343MB", "created": "2021-12-02 22:44:23 +0100 CET", "id": "0cc1cbb6d08d"}\n'
                b'{"name": "mcr.microsoft.com/vscode/devcontainers/python", "tag": "0-3.6", "size": "1.22GB", "created": "2021-12-02 22:17:47 +0100 CET", "id": "cc5abbf52b04"}\n'
            )
        ],
    )

    arguments = Namespace(container_system='capsulegirl')

    images = list_images(arguments)

    assert check_output.call_count == 1
    args, _ = check_output.call_args_list[-1]
    assert args[0] == [
        'capsulegirl',
        'image',
        'ls',
        '--format',
        '{"name": "{{.Repository}}", "tag": "{{.Tag}}", "size": "{{.Size}}", "created": "{{.CreatedAt}}", "id": "{{.ID}}"}',
    ]

    assert len(images.keys()) == 2
    assert sorted(images.get('mcr.microsoft.com/vscode/devcontainers/python', {}).keys()) == sorted(
        [
            '0-3.10',
            '0-3.10-bullseye',
            '0-3.9',
            '0-3.8',
            '0-3.6',
        ]
    )
    assert sorted(images.get('mcr.microsoft.com/vscode/devcontainers/base', {}).keys()) == sorted(
        [
            '0-focal',
        ]
    )


def test_get_default_mtu(mocker: MockerFixture) -> None:
    check_output = mocker.patch(
        'grizzly_cli.utils.subprocess.check_output',
        side_effect=[
            JSONDecodeError,
            (
                b'{"com.docker.network.bridge.default_bridge":"true","com.docker.network.bridge.enable_icc":"true",'
                b'"com.docker.network.bridge.enable_ip_masquerade":"true","com.docker.network.bridge.host_binding_ipv4":"0.0.0.0",'
                b'"com.docker.network.bridge.name":"docker0","com.docker.network.driver.mtu":"1500"}\n'
            ),
            (
                b'{"com.docker.network.bridge.default_bridge":"true","com.docker.network.bridge.enable_icc":"true",'
                b'"com.docker.network.bridge.enable_ip_masquerade":"true","com.docker.network.bridge.host_binding_ipv4":"0.0.0.0",'
                b'"com.docker.network.bridge.name":"docker0","com.docker.network.driver.mtu":"1440"}\n'
            ),
        ],
    )

    arguments = Namespace(container_system='capsulegirl')

    assert get_default_mtu(arguments) is None  # JSONDecodeError

    assert check_output.call_count == 1
    args, _ = check_output.call_args_list[-1]
    assert args[0] == [
        'capsulegirl',
        'network',
        'inspect',
        'bridge',
        '--format',
        '{{ json .Options }}',
    ]

    assert get_default_mtu(arguments) == '1500'
    assert get_default_mtu(arguments) == '1440'

    assert check_output.call_count == 3


def test_run_command(capsys: CaptureFixture, mocker: MockerFixture) -> None:
    setup_logging()

    terminate = mocker.patch('grizzly_cli.utils.subprocess.Popen.terminate', autospec=True)
    wait = mocker.patch('grizzly_cli.utils.subprocess.Popen.wait', autospec=True)

    def popen___init___no_stdout(*args: Any, **_kwargs: Any) -> None:
        args[0].returncode = 133
        args[0].stdout = None

    mocker.patch('grizzly_cli.utils.subprocess.Popen.__init__', popen___init___no_stdout)
    poll_mock = mocker.patch('grizzly_cli.utils.subprocess.Popen.poll', side_effect=[None])
    kill_mock = mocker.patch('grizzly_cli.utils.subprocess.Popen.kill', side_effect=[RuntimeError, None])

    assert run_command(['hello', 'world'], verbose=True).return_code == 133

    capture = capsys.readouterr()
    assert capture.out == ''
    assert capture.err == 'run_command: hello world\n'

    assert terminate.call_count == 1
    assert wait.call_count == 1
    assert poll_mock.call_count == 1
    assert kill_mock.call_count == 1

    def mock_command_output(output: list[str], returncode: int = 0) -> None:
        output_buffer: list[Union[bytes, int]] = [f'{line}\n'.encode() for line in output] + [0]

        def popen___init__(*args: Any, **_kwargs: Any) -> None:
            args[0].returncode = returncode

            class Stdout:
                def readline(self) -> Union[bytes, int]:
                    return output_buffer.pop(0)

            args[0].stdout = Stdout()

        mocker.patch('grizzly_cli.utils.subprocess.Popen.terminate', side_effect=[KeyboardInterrupt])
        mocker.patch('grizzly_cli.utils.subprocess.Popen.__init__', popen___init__)

    mock_command_output(
        [
            'first line',
            'second line',
        ]
    )
    poll_mock = mocker.patch('grizzly_cli.utils.subprocess.Popen.poll', side_effect=[None] * 3)

    result = run_command([], {})
    assert result.return_code == 0
    assert result.output is None
    assert result.abort_timestamp is None

    capture = capsys.readouterr()
    assert capture.out == ''
    assert capture.err == ('first line\nsecond line\n')

    assert wait.call_count == 2
    assert poll_mock.call_count == 3
    assert kill_mock.call_count == 2

    mock_command_output(
        [
            'hello world',
            'foo bar',
            'bar grizzly.returncode=1234 foo',
            'grizzly.returncode=4321',
            'world foo hello bar',
        ],
        4321,
    )
    poll_mock = mocker.patch('grizzly_cli.utils.subprocess.Popen.poll', side_effect=[None] * 6)

    result = run_command([], {}, silent=True)
    assert result.return_code == 4321
    assert result.output == [
        b'hello world\n',
        b'foo bar\n',
        b'bar grizzly.returncode=1234 foo\n',
        b'grizzly.returncode=4321\n',
        b'world foo hello bar\n',
    ]

    capture = capsys.readouterr()
    assert capture.err == ''
    assert capture.out == ''

    assert wait.call_count == 3
    assert poll_mock.call_count == 6
    assert kill_mock.call_count == 3


def test_get_distributed_system(capsys: CaptureFixture, mocker: MockerFixture) -> None:
    which = mocker.patch('grizzly_cli.utils.which')
    getstatusoutput = mocker.patch('grizzly_cli.utils.subprocess.getstatusoutput')

    # test 1
    which.side_effect = [None, None]
    getstatusoutput.return_value = (1, 'foobar')
    assert get_distributed_system() is None  # neither
    capture = capsys.readouterr()
    assert capture.out == 'neither "podman" nor "docker" found in PATH\n'
    assert which.call_count == 2
    getstatusoutput.assert_not_called()
    which.reset_mock()

    # test 2
    which.side_effect = [None, 'podman']
    getstatusoutput.return_value = (1, 'foobar')
    assert get_distributed_system() is None
    capture = capsys.readouterr()
    assert which.call_count == 2
    getstatusoutput.assert_called_once_with('podman compose version')
    assert capture.out == (
        '!! podman might not work due to buildah missing support for `RUN --mount=type=ssh`: https://github.com/containers/buildah/issues/2835\n'
        '"podman compose" not found in PATH\n'
    )
    which.reset_mock()
    getstatusoutput.reset_mock()

    # test 3
    which.side_effect = [None, 'podman']
    getstatusoutput.return_value = (0, 'foobar')
    assert get_distributed_system() == 'podman'
    capture = capsys.readouterr()
    assert which.call_count == 2
    getstatusoutput.assert_called_once_with('podman compose version')
    assert capture.out == ('!! podman might not work due to buildah missing support for `RUN --mount=type=ssh`: https://github.com/containers/buildah/issues/2835\n')
    which.reset_mock()
    getstatusoutput.reset_mock()

    # test 4
    which.side_effect = ['docker']
    getstatusoutput.return_value = (1, 'foobar')
    assert get_distributed_system() is None
    capture = capsys.readouterr()
    assert which.call_count == 1
    getstatusoutput.assert_called_once_with('docker compose version')
    assert capture.out == ('"docker compose" not found in PATH\n')
    which.reset_mock()
    getstatusoutput.reset_mock()

    # test 5
    which.side_effect = ['docker']
    getstatusoutput.return_value = (0, 'foobar')
    assert get_distributed_system() == 'docker'
    capture = capsys.readouterr()
    assert which.call_count == 1
    getstatusoutput.assert_called_once_with('docker compose version')
    assert capture.out == ''
    which.reset_mock()


def test_find_variable_names_in_questions(mocker: MockerFixture) -> None:
    mocker.patch('grizzly_cli.SCENARIOS', [])
    mocker.patch('grizzly_cli.utils.parse_feature_file', autospec=True)

    assert find_variable_names_in_questions('test.feature') == []

    mocker.patch(
        'grizzly_cli.SCENARIOS',
        [
            create_scenario(
                'scenario-1',
                [],
                [
                    'Given a user of type "RestApi" load testing "https://localhost"',
                    'And ask for value of variable test_variable_1',
                ],
            ),
        ],
    )

    with pytest.raises(ValueError, match='could not find variable name in "ask for value of variable test_variable_1'):
        find_variable_names_in_questions('test.feature')

    mocker.patch(
        'grizzly_cli.SCENARIOS',
        [
            create_scenario(
                'scenario-1',
                [],
                [
                    'Given a user of type "RestApi" load testing "https://localhost"',
                    'And ask for value of variable "test_variable_2"',
                    'And ask for value of variable "test_variable_1"',
                ],
            ),
            create_scenario(
                'scenario-2',
                [
                    'And ask for value of variable "bar"',
                ],
                [
                    'Given a user of type "MessageQueueUser" load testing "mqs://localhost"',
                    'And ask for value of variable "foo"',
                ],
            ),
        ],
    )
    variables = find_variable_names_in_questions('test.feature')
    assert len(variables) == 4
    assert variables == ['bar', 'foo', 'test_variable_1', 'test_variable_2']


def test_find_metadata_notices(tmp_path_factory: TempPathFactory) -> None:
    test_context = tmp_path_factory.mktemp('test_context')

    try:
        feature_file = test_context / 'test-1.feature'
        feature_file.write_text("""Feature: test -1
    Scenario: hello world
        Given a feature file with a rich set of expressions
""")
        assert find_metadata_notices(str(feature_file)) == []

        feature_file.write_text("""# grizzly-cli run --verbose
# grizzly-cli:notice have you created testdata?
Feature: test -1
    Scenario: hello world
        Given a feature file with a rich set of expressions
""")

        assert find_metadata_notices(str(feature_file)) == ['have you created testdata?']

        feature_file.write_text("""# grizzly-cli run --verbose
# grizzly-cli:notice have you created testdata?
Feature: test -1
    Scenario: hello world
        # grizzly-cli:notice is the event log cleared?
        Given a feature file with a rich set of expressions
""")

        assert find_metadata_notices(str(feature_file)) == ['have you created testdata?', 'is the event log cleared?']
    finally:
        rm_rf(test_context)


def test_distribution_of_users_per_scenario(capsys: CaptureFixture, mocker: MockerFixture) -> None:  # noqa: PLR0915
    setup_logging()

    arguments = Namespace(file='test.feature', yes=False)

    ask_yes_no = mocker.patch('grizzly_cli.utils.ask_yes_no', autospec=True)

    mocker.patch(
        'grizzly_cli.SCENARIOS',
        [
            create_scenario(
                'scenario-1',
                [],
                [
                    'Given a user of type "RestApi" load testing "https://localhost"',
                    'And ask for value of variable "test_variable_2"',
                    'And ask for value of variable "test_variable_1"',
                ],
            ),
        ],
    )

    with pytest.raises(ValueError, match='grizzly needs at least 1 users to run this feature'):
        distribution_of_users_per_scenario(arguments, {})

    mocker.patch(
        'grizzly_cli.SCENARIOS',
        [
            create_scenario(
                'scenario-1',
                [
                    'Given "10" users',
                ],
                [
                    'Given a user of type "RestApi" load testing "https://localhost"',
                    'And repeat for "1" iteration',
                    'And ask for value of variable "test_variable_2"',
                    'And ask for value of variable "test_variable_1"',
                ],
            ),
            create_scenario(
                'scenario-2',
                [
                    'And ask for value of variable "bar"',
                ],
                [
                    'Given a user of type "MessageQueueUser" load testing "mqs://localhost"',
                    'And repeat for "1" iteration',
                    'And ask for value of variable "foo"',
                ],
            ),
        ],
    )

    with pytest.raises(ValueError, match='scenario-1 will have 5 users to run 1 iterations, increase iterations or lower user count'):
        distribution_of_users_per_scenario(arguments, {})

    capsys.readouterr()

    mocker.patch(
        'grizzly_cli.SCENARIOS',
        [
            create_scenario(
                'scenario-1',
                [
                    'Given "2" users',
                ],
                [
                    'Given a user of type "RestApi" load testing "https://localhost"',
                    'And repeat for "1" iteration',
                    'And ask for value of variable "test_variable_2"',
                    'And ask for value of variable "test_variable_1"',
                ],
            ),
            create_scenario(
                'scenario-2',
                [
                    'And ask for value of variable "bar"',
                ],
                [
                    'Given a user of type "MessageQueueUser" load testing "mqs://localhost"',
                    'And repeat for "1" iteration',
                    'And ask for value of variable "foo"',
                ],
            ),
        ],
    )

    distribution_of_users_per_scenario(arguments, {})
    capture = capsys.readouterr()

    assert capture.out == ''
    expected_lines = [
        '\n',
        'feature file test.feature will execute in total 2 iterations divided on 2 scenarios\n',
        '\n',
        'each scenario will execute accordingly:\n',
        '\n',
        'ident   weight  #iter  #user  description\n',
        '------|-------|------|------|-------------|\n',
        '001          1      1      1  scenario-1 \n',
        '002          1      1      1  scenario-2 \n',
        '------|-------|------|------|-------------|\n',
        '\n',
    ]
    assert capture.err == ''.join(expected_lines)
    capsys.readouterr()
    assert ask_yes_no.call_count == 1
    args, _ = ask_yes_no.call_args_list[-1]
    assert args[0] == 'continue?'

    mocker.patch(
        'grizzly_cli.SCENARIOS',
        [
            create_scenario(
                'scenario-1',
                [],
                [],
            ),
        ],
    )

    with pytest.raises(ValueError, match='scenario "scenario-1" does not have any steps'):
        distribution_of_users_per_scenario(arguments, {})

    mocker.patch(
        'grizzly_cli.SCENARIOS',
        [
            create_scenario(
                'scenario-1',
                ['Given "1" users'],
                ['And repeat for "10" iterations'],
            ),
        ],
    )

    with pytest.raises(ValueError, match='scenario-1 does not have a user type'):
        distribution_of_users_per_scenario(arguments, {})

    mocker.patch(
        'grizzly_cli.SCENARIOS',
        [
            create_scenario(
                'scenario-1',
                [
                    'Given "{{ users }}" users',
                ],
                [
                    'Given a user of type "RestApi" with weight "{{ integer }}" load testing "https://localhost"',
                    'And repeat for "{{ integer * 0.10 }}" iterationsAnd ask for value of variable "test_variable_2"',
                    'And ask for value of variable "test_variable_1"',
                ],
            ),
            create_scenario(
                'scenario-2',
                [
                    'And ask for value of variable "bar"',
                ],
                [
                    'Given a user of type "MessageQueueUser" load testing "mqs://localhost"',
                    'And repeat for "{{ integer * 0.01 }}" iterations',
                    'And ask for value of variable "foo"',
                ],
            ),
        ],
    )

    import grizzly_cli.utils

    render = mocker.spy(grizzly_cli.utils.Template, 'render')  # type: ignore[attr-defined]

    distribution_of_users_per_scenario(
        arguments,
        {
            'TESTDATA_VARIABLE_users': '40',
            'TESTDATA_VARIABLE_boolean': 'True',
            'TESTDATA_VARIABLE_integer': '500',
            'TESTDATA_VARIABLE_float': '1.33',
            'TESTDATA_VARIABLE_string': 'foo bar',
            'TESTDATA_VARIABLE_neg_integer': '-100',
            'TESTDATA_VARIABLE_neg_float': '-1.33',
            'TESTDATA_VARIABLE_pad_integer': '001',
        },
    )
    capture = capsys.readouterr()

    assert capture.out == ''
    assert (
        capture.err
        == """
feature file test.feature will execute in total 55 iterations divided on 2 scenarios

each scenario will execute accordingly:

ident   weight  #iter  #user  description
------|-------|------|------|-------------|
001        500     50     39  scenario-1 
002          1      5      1  scenario-2 
------|-------|------|------|-------------|

"""
    )
    capsys.readouterr()
    assert ask_yes_no.call_count == 2
    args, _ = ask_yes_no.call_args_list[-1]
    assert args[0] == 'continue?'

    assert render.call_count == 5
    for _, kwargs in render.call_args_list:
        assert kwargs.get('boolean', None)
        assert kwargs.get('integer', None) == 500
        assert kwargs.get('float', None) == 1.33
        assert kwargs.get('string', None) == 'foo bar'
        assert kwargs.get('neg_integer', None) == -100
        assert kwargs.get('neg_float', None) == -1.33
        assert kwargs.get('pad_integer', None) == '001'

    mocker.patch(
        'grizzly_cli.SCENARIOS',
        [
            create_scenario(
                'scenario-1 testing a lot of stuff',
                [
                    'Given "70" users',
                ],
                [
                    'Given a user of type "RestApi" with weight "100" load testing "https://localhost"',
                    'And repeat for "500" iterationsAnd ask for value of variable "test_variable_2"',
                    'And ask for value of variable "test_variable_1"',
                ],
            ),
            create_scenario(
                'scenario-2 testing a lot more of many different things that scenario-1 does not test',
                [
                    'And ask for value of variable "bar"',
                ],
                [
                    'Given a user of type "MessageQueueUser" with weight "50" load testing "mqs://localhost"',
                    'And repeat for "750" iterations',
                    'And ask for value of variable "foo"',
                ],
            ),
            create_scenario(
                'scenario-3',
                [],
                [
                    'Given a user of type "RestApi" with weight "1" load testing "https://127.0.0.2"',
                    'And repeat for "10" iterations',
                ],
            ),
        ],
    )

    arguments = Namespace(file='integration.feature', yes=True)

    distribution_of_users_per_scenario(arguments, {})
    capture = capsys.readouterr()

    assert capture.out == ''
    assert (
        capture.err
        == """
feature file integration.feature will execute in total 1260 iterations divided on 3 scenarios

each scenario will execute accordingly:

ident   weight  #iter  #user  description                                                                         
------|-------|------|------|--------------------------------------------------------------------------------------|
001        100    500     46  scenario-1 testing a lot of stuff                                                   
002         50    750     23  scenario-2 testing a lot more of many different things that scenario-1 does not test
003          1     10      1  scenario-3                                                                          
------|-------|------|------|--------------------------------------------------------------------------------------|

"""
    )
    capsys.readouterr()
    assert ask_yes_no.call_count == 2

    mocker.patch(
        'grizzly_cli.SCENARIOS',
        [
            create_scenario(
                'scenario-1',
                [
                    'Given "1" user',
                ],
                [
                    'Given a user of type "RestApi" with weight "25" load testing "https://localhost"',
                    'And repeat for "1" iterations',
                ],
            ),
        ],
    )

    distribution_of_users_per_scenario(arguments, {})
    capture = capsys.readouterr()

    assert capture.out == ''
    assert (
        capture.err
        == """
feature file integration.feature will execute in total 1 iterations divided on 1 scenarios

each scenario will execute accordingly:

ident   weight  #iter  #user  description
------|-------|------|------|-------------|
001         25      1      1  scenario-1 
------|-------|------|------|-------------|

"""
    )
    capsys.readouterr()

    arguments = Namespace(file='integration.feature', yes=True, environment_file='environments/local.yaml')
    distribution_of_users_per_scenario(arguments, {'GRIZZLY_CONFIGURATION_FILE': 'environments/local.lock.yaml'})
    capture = capsys.readouterr()

    assert capture.out == ''
    assert (
        capture.err
        == """
feature file integration.feature will execute in total 1 iterations divided on 1 scenarios with environment file environments/local.lock.yaml

each scenario will execute accordingly:

ident   weight  #iter  #user  description
------|-------|------|------|-------------|
001         25      1      1  scenario-1 
------|-------|------|------|-------------|

"""
    )
    capsys.readouterr()

    mocker.patch(
        'grizzly_cli.SCENARIOS',
        [
            create_scenario(
                'scenario-1 testing a lot of stuff',
                [
                    'Given "4" users',
                ],
                [
                    'Given a user of type "RestApi" with weight "1" load testing "https://localhost"',
                    'And repeat for "10" iterationsAnd ask for value of variable "test_variable_2"',
                    'And ask for value of variable "test_variable_1"',
                ],
            ),
            create_scenario(
                'scenario-2 testing a lot more of many different things that scenario-1 does not test',
                [],
                [
                    'Given a user of type "MessageQueueUser" with weight "50" load testing "mqs://localhost"',
                    'And repeat for "0" iterations',
                    'And ask for value of variable "foo"',
                ],
            ),
            create_scenario(
                'scenario-3',
                [],
                [
                    'Given a user of type "RestApi" with weight "0" load testing "https://127.0.0.2"',
                    'And repeat for "10" iterations',
                ],
            ),
            create_scenario(
                'scenario-4',
                [],
                [
                    'Given a user of type "RestApi" with weight "0" load testing "https://127.0.0.2"',
                    'And repeat for "0" iterations',
                ],
            ),
        ],
    )

    arguments = Namespace(file='integration.feature', yes=True)

    with pytest.raises(ValueError) as ve:  # noqa: PT011
        distribution_of_users_per_scenario(arguments, {})
    capture = capsys.readouterr()

    assert capture.out == ''
    assert (
        capture.err
        == """
feature file integration.feature will execute in total 20 iterations divided on 4 scenarios

each scenario will execute accordingly:

ident   weight  #iter  #user  description                                                                            errors
------|-------|------|------|--------------------------------------------------------------------------------------|----------------------------------|
001          1     10      1  scenario-1 testing a lot of stuff                                                      
002         50      0      3  scenario-2 testing a lot more of many different things that scenario-1 does not test   no iterations
003          0     10      0  scenario-3                                                                             no users assigned
004          0      0      0  scenario-4                                                                             no users assigned, no iterations
------|-------|------|------|--------------------------------------------------------------------------------------|----------------------------------|
"""
    )
    assert (
        str(ve.value)
        == """                                                                                                                    ^
+-------------------------------------------------------------------------------------------------------------------+
|
+- there were errors when calculating user distribution and iterations per scenario, adjust user "weight", number of users or iterations per scenario\n"""
    )


@pytest.mark.parametrize(
    ('users', 'iterations', 'output'),
    [
        (
            6,
            13,
            """
feature file integration.feature will execute in total 28 iterations divided on 7 scenarios

each scenario will execute accordingly:

ident   weight  #iter  #user  description
------|-------|------|------|-------------|
001         50     15      5  scenario-0 
002         12      3      1  scenario-1 
003         21      5      2  scenario-2 
004          4      1      1  scenario-3 
005          6      2      1  scenario-4 
006          3      1      1  scenario-5 
007          3      1      1  scenario-6 
------|-------|------|------|-------------|

""",
        ),
        (
            12,
            20,
            """
feature file integration.feature will execute in total 43 iterations divided on 7 scenarios

each scenario will execute accordingly:

ident   weight  #iter  #user  description
------|-------|------|------|-------------|
001         33     23      6  scenario-0 
002         16      5      2  scenario-1 
003         28      8      5  scenario-2 
004          5      2      1  scenario-3 
005          8      3      2  scenario-4 
006          4      1      1  scenario-5 
007          4      1      1  scenario-6 
------|-------|------|------|-------------|

""",
        ),
        (
            18,
            31,
            """
feature file integration.feature will execute in total 66 iterations divided on 7 scenarios

each scenario will execute accordingly:

ident   weight  #iter  #user  description
------|-------|------|------|-------------|
001         25     35      6  scenario-0 
002         18      8      4  scenario-1 
003         31     13      7  scenario-2 
004          6      2      2  scenario-3 
005          9      4      3  scenario-4 
006          4      2      1  scenario-5 
007          4      2      1  scenario-6 
------|-------|------|------|-------------|

""",
        ),
        (
            24,
            49,
            """
feature file integration.feature will execute in total 105 iterations divided on 7 scenarios

each scenario will execute accordingly:

ident   weight  #iter  #user  description
------|-------|------|------|-------------|
001         20     56      6  scenario-0 
002         20     12      6  scenario-1 
003         33     21     10  scenario-2 
004          6      4      1  scenario-3 
005         10      6      3  scenario-4 
006          4      3      2  scenario-5 
007          4      3      2  scenario-6 
------|-------|------|------|-------------|

""",
        ),
        (
            30,
            58,
            """
feature file integration.feature will execute in total 124 iterations divided on 7 scenarios

each scenario will execute accordingly:

ident   weight  #iter  #user  description
------|-------|------|------|-------------|
001         16     66      6  scenario-0 
002         20     15      7  scenario-1 
003         35     24     12  scenario-2 
004          6      5      3  scenario-3 
005         10      8      4  scenario-4 
006          5      3      2  scenario-5 
007          5      3      2  scenario-6 
------|-------|------|------|-------------|

""",
        ),
        (
            30,
            21000,
            """
feature file integration.feature will execute in total 44940 iterations divided on 7 scenarios

each scenario will execute accordingly:

ident   weight  #iter  #user  description
------|-------|------|------|-------------|
001         16  23940      6  scenario-0 
002         20   5250      7  scenario-1 
003         35   8820     12  scenario-2 
004          6   1680      3  scenario-3 
005         10   2730      4  scenario-4 
006          5   1260      2  scenario-5 
007          5   1260      2  scenario-6 
------|-------|------|------|-------------|

""",
        ),
    ],
)
def test_distribution_of_users_per_scenario_advanced(capsys: CaptureFixture, mocker: MockerFixture, users: int, iterations: int, output: str) -> None:
    setup_logging()

    # all scenarios in a feature file will, at this point, have all the background steps
    # grizzly will later make sure that they are only run once
    background_steps = [
        'Given "{{ (users | int) + 6 }}" users',
        'And spawn rate is "{{ rate }}" users per second',
    ]

    mocker.patch(
        'grizzly_cli.SCENARIOS',
        [
            create_scenario(
                'scenario-0',
                background_steps,
                [
                    'Given a user of type "RestApi" with weight "{{ (6 / ((users | int) + 6) + 0.5 | int) * 100 }}" load testing "https://localhost"',
                    'And repeat for "{{ (leveranser | int) + ((((leveranser * 0.06) + 0.5) | int) or 1) + ((((leveranser * 0.08) + 0.5) | int) or 1) }}" iterations',
                ],
            ),
            create_scenario(
                'scenario-1',
                background_steps,
                [
                    'Given a user of type "RestApi" with weight "{{ (100 - ((6 / ((users | int) + 6) + 0.5 | int) * 100)) * 0.25 }}" load testing "https://localhost"',
                    'And repeat for "{{ ((leveranser * 0.25) + 0.5) | int }}" iterations',
                ],
            ),
            create_scenario(
                'scenario-2',
                background_steps,
                [
                    'Given a user of type "RestApi" with weight "{{ (100 - ((6 / ((users | int) + 6) + 0.5 | int) * 100)) * 0.42 }}" load testing "https://localhost"',
                    'And repeat for "{{ ((leveranser * 0.42) + 0.5) | int }}" iterations',
                ],
            ),
            create_scenario(
                'scenario-3',
                background_steps,
                [
                    'Given a user of type "RestApi" with weight "{{ (100 - ((6 / ((users | int) + 6) + 0.5 | int) * 100)) * 0.08 }}" load testing "https://localhost"',
                    'And repeat for "{{ ((leveranser * 0.08) + 0.5) | int }}" iterations',
                ],
            ),
            create_scenario(
                'scenario-4',
                background_steps,
                [
                    'Given a user of type "RestApi" with weight "{{ (100 - ((6 / ((users | int) + 6) + 0.5 | int) * 100)) * 0.13 }}" load testing "https://localhost"',
                    'And repeat for "{{ ((leveranser * 0.13) + 0.5) | int }}" iterations',
                ],
            ),
            create_scenario(
                'scenario-5',
                background_steps,
                [
                    'Given a user of type "RestApi" with weight "{{ (100 - ((6 / ((users | int) + 6) + 0.5 | int) * 100)) * 0.06 }}" load testing "https://localhost"',
                    'And repeat for "{{ ((leveranser * 0.06) + 0.5) | int }}" iterations',
                ],
            ),
            create_scenario(
                'scenario-6',
                background_steps,
                [
                    'Given a user of type "RestApi" with weight "{{ (100 - ((6 / ((users | int) + 6) + 0.5 | int) * 100)) * 0.06 }}" load testing "https://localhost"',
                    'And repeat for "{{ ((leveranser * 0.06) + 0.5) | int }}" iterations',
                ],
            ),
        ],
    )

    arguments = Namespace(file='integration.feature', yes=True)

    distribution_of_users_per_scenario(
        arguments,
        {
            'TESTDATA_VARIABLE_leveranser': f'{iterations}',
            'TESTDATA_VARIABLE_users': f'{users}',
            'TESTDATA_VARIABLE_rate': f'{users}',
        },
    )
    capture = capsys.readouterr()

    assert capture.out == ''
    assert capture.err == output
    capsys.readouterr()


def test_distribution_of_users_per_scenario_no_weights(capsys: CaptureFixture, mocker: MockerFixture) -> None:
    setup_logging()

    # all scenarios in a feature file will, at this point, have all the background steps
    # grizzly will later make sure that they are only run once
    background_steps = [
        'Given spawn rate is "{{ rate }}" users per second',
    ]

    mocker.patch(
        'grizzly_cli.SCENARIOS',
        [
            create_scenario(
                'scenario-0',
                background_steps,
                [
                    'Given "{{ ((((max_users * 0.7) - 0.5) | int) or 1) if max_users is defined else 1 }}" users of type "RestApi" load testing "https://localhost"',
                    'And repeat for "{{ (leveranser | int) + ((((leveranser * 0.7) + 0.5) | int) or 1) + ((((leveranser * 0.3) + 0.5) | int) or 1) }}" iterations',
                ],
            ),
            create_scenario(
                'scenario-1',
                background_steps,
                [
                    'Given "{{ ((((max_users_undefined * 0.3) - 0.5) | int) or 1) if max_users_undefined is defined else 1 }}" user of type "RestApi" load testing "https://localhost"',
                    'And repeat for "{{ ((leveranser * 0.3) + 0.5) | int }}" iterations',
                ],
            ),
        ],
    )

    arguments = Namespace(file='integration.feature', yes=True)

    distribution_of_users_per_scenario(
        arguments,
        {
            'TESTDATA_VARIABLE_leveranser': '10',
            'TESTDATA_VARIABLE_max_users': '10',
            'TESTDATA_VARIABLE_rate': '10',
        },
    )
    capture = capsys.readouterr()

    assert capture.out == ''
    assert (
        capture.err
        == """
feature file integration.feature will execute in total 23 iterations divided on 2 scenarios

each scenario will execute accordingly:

ident   #iter  #user  description
------|------|------|-------------|
001        20      6  scenario-0 
002         3      1  scenario-1 
------|------|------|-------------|

"""
    )
    capsys.readouterr()


def test_ask_yes_no(capsys: CaptureFixture, mocker: MockerFixture) -> None:
    get_input = mocker.patch('grizzly_cli.utils.get_input', side_effect=['yeah', 'n', 'y'])

    with pytest.raises(KeyboardInterrupt):
        ask_yes_no('continue?')

    capture = capsys.readouterr()
    assert capture.err == ''
    assert capture.out == 'you must answer y (yes) or n (no)\n'

    assert get_input.call_count == 2
    for args, _ in get_input.call_args_list:
        assert args[0] == 'continue? [y/n]: '
    get_input.reset_mock()

    ask_yes_no('are you sure you know what you are doing?')
    capture = capsys.readouterr()
    assert capture.err == ''
    assert capture.out == ''

    assert get_input.call_count == 1
    for args, _ in get_input.call_args_list:
        assert args[0] == 'are you sure you know what you are doing? [y/n]: '


def test_get_dependency_versions_git(mocker: MockerFixture, tmp_path_factory: TempPathFactory, capsys: CaptureFixture) -> None:  # noqa: PLR0915
    test_context = tmp_path_factory.mktemp('test_context')
    requirements_file = test_context / 'requirements.txt'

    mocker.patch('grizzly_cli.EXECUTION_CONTEXT', str(test_context))

    try:
        grizzly_versions, locust_version = get_dependency_versions(local_install=False)

        assert grizzly_versions == (None, None)
        assert locust_version is None

        requirements_file.touch()

        assert get_dependency_versions(local_install=False) == (('(unknown)', None), '(unknown)')

        capture = capsys.readouterr()
        assert capture.err == f'!! unable to find grizzly dependency in {requirements_file.absolute()}\n'
        assert capture.out == ''

        requirements_file.write_text('git+https://github.com/Biometria-se/grizzly.git@v1.5.3#egg=grizzly-loadtester')
        import subprocess

        with ExitStack() as stack:
            stack.enter_context(
                mocker.patch.context_manager(subprocess, 'check_call', side_effect=[1, 0, 0, 1, 0]),
            )
            stack.enter_context(
                mocker.patch.context_manager(
                    subprocess,
                    'check_output',
                    side_effect=[
                        subprocess.CalledProcessError(returncode=1, cmd=''),
                        'main\n',
                        'branch\n',
                        'v1.5.3\n',
                    ],
                ),
            )

            assert get_dependency_versions(local_install=False) == (('(unknown)', None), '(unknown)')

            capture = capsys.readouterr()
            assert capture.err == '!! unable to clone git repo https://github.com/Biometria-se/grizzly.git\n'
            assert capture.out == ''

            assert subprocess.check_call.call_count == 1  # type: ignore[attr-defined]
            assert subprocess.check_output.call_count == 0  # type: ignore[attr-defined]

            # git clone...
            args, kwargs = subprocess.check_call.call_args_list[0]  # type: ignore[attr-defined]
            assert len(args) == 1
            args = args[0]
            assert args[:-1] == ['git', 'clone', '--filter=blob:none', '-q', 'https://github.com/Biometria-se/grizzly.git']
            assert isinstance(args[-1], Path)
            assert args[-1].as_posix().startswith(Path(gettempdir()).as_posix())
            assert args[-1].as_posix().endswith('grizzly-loadtester_3f210f1809f6ca85ef414b2b4d450bf54353b5e0')
            assert not kwargs.get('shell', True)
            assert kwargs.get('stdout', None) == subprocess.DEVNULL
            assert kwargs.get('stderr', None) == subprocess.DEVNULL

            assert get_dependency_versions(local_install=False) == (('(unknown)', None), '(unknown)')

            capture = capsys.readouterr()
            assert capture.err == '!! unable to check branch name of HEAD in git repo https://github.com/Biometria-se/grizzly.git\n'
            assert capture.out == ''

            assert subprocess.check_call.call_count == 2  # type: ignore[attr-defined]
            assert subprocess.check_output.call_count == 1  # type: ignore[attr-defined]

            # git rev-parse...
            args, kwargs = subprocess.check_output.call_args_list[0]  # type: ignore[attr-defined]
            assert len(args) == 1
            args = args[0]
            assert args == ['git', 'rev-parse', '--abbrev-ref', 'HEAD']
            assert not kwargs.get('shell', True)
            kwarg_cwd = kwargs.get('cwd', None)
            assert isinstance(kwarg_cwd, Path)
            assert kwarg_cwd.as_posix().startswith(Path(gettempdir()).as_posix())
            assert kwarg_cwd.as_posix().endswith('grizzly-loadtester_3f210f1809f6ca85ef414b2b4d450bf54353b5e0')
            assert kwargs.get('universal_newlines', False)

            assert get_dependency_versions(local_install=True) == (('(unknown)', None), '(unknown)')

            capture = capsys.readouterr()
            assert capture.err == '!! unable to checkout branch v1.5.3 from git repo https://github.com/Biometria-se/grizzly.git\n'
            assert capture.out == ''

            assert subprocess.check_call.call_count == 4  # type: ignore[attr-defined]
            assert subprocess.check_output.call_count == 3  # type: ignore[attr-defined]

            # git checkout...
            args, kwargs = subprocess.check_call.call_args_list[-1]  # type: ignore[attr-defined]
            assert len(args) == 1
            args = args[0]
            assert args == ['git', 'checkout', '-b', 'v1.5.3', '--track', 'origin/v1.5.3']
            kwarg_cwd = kwargs.get('cwd', None)
            assert kwarg_cwd.as_posix().startswith(Path(gettempdir()).as_posix())
            assert kwarg_cwd.as_posix().endswith('grizzly-loadtester_3f210f1809f6ca85ef414b2b4d450bf54353b5e0')
            assert not kwargs.get('shell', True)
            assert kwargs.get('stdout', None) == subprocess.DEVNULL
            assert kwargs.get('stderr', None) == subprocess.DEVNULL

            with pytest.raises(FileNotFoundError):
                get_dependency_versions(local_install=False)

            capture = capsys.readouterr()
            assert capture.err == ''
            assert capture.out == ''

            assert subprocess.check_call.call_count == 5  # type: ignore[attr-defined]
            assert subprocess.check_output.call_count == 4  # type: ignore[attr-defined]

        with ExitStack() as stack:
            stack.enter_context(mocker.patch.context_manager(subprocess, 'check_call', return_value=0))
            stack.enter_context(
                mocker.patch.context_manager(subprocess, 'check_output', return_value='main\n'),
            )

            with pytest.raises(FileNotFoundError) as fne:
                get_dependency_versions(local_install=False)
            assert fne.value.errno == 2
            assert fne.value.strerror == 'No such file or directory'

            with unittest_patch(
                'grizzly_cli.utils.Path.open',
                side_effect=[
                    mock_open(read_data='git+https://github.com/Biometria-se/grizzly.git@v1.5.3#egg=grizzly-loadtester\n').return_value,
                    mock_open(read_data='').return_value,
                ],
            ) as open_mock:
                assert get_dependency_versions(local_install=False) == (('(unknown)', None), '(unknown)')

                capture = capsys.readouterr()
                assert capture.err == '!! unable to find "__version__" declaration in grizzly/__init__.py from https://github.com/Biometria-se/grizzly.git\n'
                assert capture.out == ''
                assert open_mock.call_count == 2

            with unittest_patch(
                'grizzly_cli.utils.Path.open',
                side_effect=[
                    mock_open(read_data='git+https://github.com/Biometria-se/grizzly.git@v1.5.3#egg=grizzly-loadtester\n').return_value,
                    mock_open(read_data="__version__ = '0.0.0'").return_value,
                    mock_open(read_data='').return_value,
                ],
            ) as open_mock:
                assert get_dependency_versions(local_install=False) == (('0.0.0', []), '(unknown)')

                capture = capsys.readouterr()
                assert capture.err == '!! unable to find "locust" dependency in requirements.txt from https://github.com/Biometria-se/grizzly.git\n'
                assert capture.out == ''

                assert open_mock.call_count == 3

            with unittest_patch(
                'grizzly_cli.utils.Path.open',
                side_effect=[
                    mock_open(read_data='git+https://github.com/Biometria-se/grizzly.git@v1.5.3#egg=grizzly-loadtester[dev,mq]\n').return_value,
                    mock_open(read_data="__version__ = '1.5.3'").return_value,
                    mock_open(read_data='locust').return_value,
                ],
            ) as open_mock:
                assert get_dependency_versions(local_install=False) == (('1.5.3', ['dev', 'mq']), '(unknown)')

                capture = capsys.readouterr()
                assert capture.err == '!! unable to find locust version in "locust" specified in requirements.txt from https://github.com/Biometria-se/grizzly.git\n'
                assert capture.out == ''

                assert open_mock.call_count == 3

            with unittest_patch(
                'grizzly_cli.utils.Path.open',
                side_effect=[
                    mock_open(read_data='git+https://github.com/Biometria-se/grizzly.git@v1.5.3#egg=grizzly-loadtester\n').return_value,
                    mock_open(read_data="__version__ = '1.5.3'").return_value,
                    mock_open(read_data='locust==2.2.1 \\ ').return_value,
                ],
            ) as open_mock:
                assert get_dependency_versions(local_install=False) == (('1.5.3', []), '2.2.1')

                capture = capsys.readouterr()
                assert capture.err == ''
                assert capture.out == ''

                assert open_mock.call_count == 3

            mocker.patch('grizzly_cli.utils.Path.exists', return_value=True)

            with pytest.raises(FileNotFoundError) as fne:
                get_dependency_versions(local_install=False)
            assert fne.value.errno == 2
            assert fne.value.strerror == 'No such file or directory'

            with unittest_patch(
                'grizzly_cli.utils.Path.open',
                side_effect=[
                    mock_open(read_data='git+https://github.com/Biometria-se/grizzly.git@main#egg=grizzly-loadtester\n').return_value,
                    mock_open(read_data='').return_value,
                ],
            ) as open_mock:
                assert get_dependency_versions(local_install=False) == (('(unknown)', None), '(unknown)')

                capture = capsys.readouterr()
                assert capture.err == '!! unable to find "version" declaration in setup.cfg from https://github.com/Biometria-se/grizzly.git\n'
                assert capture.out == ''
                assert open_mock.call_count == 2

            with unittest_patch(
                'grizzly_cli.utils.Path.open',
                side_effect=[
                    mock_open(read_data='git+https://github.com/Biometria-se/grizzly.git@main#egg=grizzly-loadtester[mq]\n').return_value,
                    mock_open(read_data='name = grizzly-loadtester\nversion = 2.0.0').return_value,
                    mock_open(read_data='locust==2.8.4 \\ ').return_value,
                ],
            ) as open_mock:
                assert get_dependency_versions(local_install=False) == (('2.0.0', ['mq']), '2.8.4')

                capture = capsys.readouterr()
                assert capture.err == ''
                assert capture.out == ''
                assert open_mock.call_count == 3

            with unittest_patch(
                'grizzly_cli.utils.Path.open',
                side_effect=[
                    mock_open(read_data='grizzly-loadtester @ git+https://github.com/Biometria-se/grizzly.git@main\n').return_value,
                    mock_open(read_data='').return_value,
                ],
            ) as open_mock:
                assert get_dependency_versions(local_install=False) == (('(unknown)', None), '(unknown)')

                capture = capsys.readouterr()
                assert capture.err == '!! unable to find "version" declaration in setup.cfg from https://github.com/Biometria-se/grizzly.git\n'
                assert capture.out == ''
                assert open_mock.call_count == 2

            with unittest_patch(
                'grizzly_cli.utils.Path.open',
                side_effect=[
                    mock_open(read_data='grizzly-loadtester[mq] @ git+https://github.com/Biometria-se/grizzly.git@main\n').return_value,
                    mock_open(read_data='name = grizzly-loadtester\nversion = 2.0.0').return_value,
                    mock_open(read_data='locust==2.8.4 \\ ').return_value,
                ],
            ) as open_mock:
                assert get_dependency_versions(local_install=False) == (('2.0.0', ['mq']), '2.8.4')

                capture = capsys.readouterr()
                assert capture.err == ''
                assert capture.out == ''
                assert open_mock.call_count == 3

            with unittest_patch(
                'grizzly_cli.utils.Path.open',
                side_effect=[
                    mock_open(read_data='grizzly-loadtester[mq] % git+https://github.com/Biometria-se/grizzly.git@main\n').return_value,
                ],
            ) as open_mock:
                assert get_dependency_versions(local_install=False) == (('(unknown)', None), '(unknown)')

                capture = capsys.readouterr()
                assert capture.err == f'!! unable to find properly formatted grizzly dependency in {requirements_file}\n'
                assert capture.out == ''
                assert open_mock.call_count == 1
    finally:
        rm_rf(test_context)


@pytest.mark.filterwarnings('ignore:Creating a LegacyVersion has been deprecated')
def test_get_dependency_versions_pypi(mocker: MockerFixture, tmp_path_factory: TempPathFactory, capsys: CaptureFixture, requests_mock: RequestsMocker) -> None:  # noqa: PLR0915
    test_context = tmp_path_factory.mktemp('test_context')
    requirements_file = test_context / 'requirements.txt'

    mocker.patch('grizzly_cli.EXECUTION_CONTEXT', str(test_context))

    try:
        grizzly_versions, locust_version = get_dependency_versions(local_install=False)

        assert grizzly_versions == (None, None)
        assert locust_version is None

        requirements_file.touch()

        assert get_dependency_versions(local_install=False) == (('(unknown)', None), '(unknown)')

        capture = capsys.readouterr()
        assert capture.err == f'!! unable to find grizzly dependency in {requirements_file.absolute()}\n'
        assert capture.out == ''

        requirements_file.write_text('grizzly-loadtester')

        requests_mock.register_uri('GET', 'https://pypi.org/pypi/grizzly-loadtester/json', status_code=404)

        assert get_dependency_versions(local_install=False) == (('(unknown)', None), '(unknown)')

        capture = capsys.readouterr()
        assert capture.err == '!! unable to get grizzly package information from https://pypi.org/pypi/grizzly-loadtester/json (404)\n'
        assert capture.out == ''

        requests_mock.register_uri('GET', 'https://pypi.org/pypi/grizzly-loadtester/json', status_code=200, text='{"info": {"version": "1.1.1"}}')
        requests_mock.register_uri('GET', 'https://pypi.org/pypi/grizzly-loadtester/1.1.1/json', status_code=400)

        assert get_dependency_versions(local_install=False) == (('1.1.1', []), '(unknown)')

        capture = capsys.readouterr()
        assert capture.err == '!! unable to get grizzly 1.1.1 package information from https://pypi.org/pypi/grizzly-loadtester/1.1.1/json (400)\n'
        assert capture.out == ''

        requests_mock.register_uri('GET', 'https://pypi.org/pypi/grizzly-loadtester/1.1.1/json', status_code=200, text='{"info": {"requires_dist": []}}')

        assert get_dependency_versions(local_install=True) == (('1.1.1', []), '(unknown)')

        capture = capsys.readouterr()
        assert capture.err == '!! could not find "locust" in requires_dist information for grizzly-loadtester 1.1.1\n'
        assert capture.out == ''

        requests_mock.register_uri('GET', 'https://pypi.org/pypi/grizzly-loadtester/1.1.1/json', status_code=200, text='{"info": {"requires_dist": ["requests", "locust"]}}')

        requirements_file.unlink()
        requirements_file.write_text('grizzly-loadtester[dev,mq]')

        actual_dependency_versions = get_dependency_versions(local_install=True)

        assert actual_dependency_versions == (('1.1.1', ['dev', 'mq']), '(unknown)')

        capture = capsys.readouterr()
        assert capture.err == '!! unable to find locust version in "locust" specified in pypi for grizzly-loadtester 1.1.1\n'
        assert capture.out == ''

        requests_mock.register_uri('GET', 'https://pypi.org/pypi/grizzly-loadtester/1.1.1/json', status_code=200, text='{"info": {"requires_dist": ["locust (==2.8.5)"]}}')

        assert get_dependency_versions(local_install=False) == (('1.1.1', ['dev', 'mq']), '2.8.5')

        capture = capsys.readouterr()
        assert capture.err == ''
        assert capture.out == ''

        requirements_file.unlink()
        requirements_file.write_text('grizzly-loadtester[mq]==1.4.0')

        requests_mock.register_uri('GET', 'https://pypi.org/pypi/grizzly-loadtester/json', status_code=200, text='{"releases": {"1.3.0": [], "1.5.0": []}}')

        assert get_dependency_versions(local_install=False) == (('(unknown)', None), '(unknown)')

        capture = capsys.readouterr()
        assert capture.err == '!! could not resolve grizzly-loadtester[mq]==1.4.0 to one specific version available at pypi\n'
        assert capture.out == ''

        requirements_file.unlink()
        requirements_file.write_text('grizzly-loadtester[mq]>1.3.0,<1.5.0')

        requests_mock.register_uri(
            'GET',
            'https://pypi.org/pypi/grizzly-loadtester/json',
            status_code=200,
            text='{"releases": {"1.3.0": [], "1.4.0": [], "1.5.0": [], "foobar": []}}',
        )
        requests_mock.register_uri('GET', 'https://pypi.org/pypi/grizzly-loadtester/1.4.0/json', status_code=200, text='{"info": {"requires_dist": ["locust (==1.0.0)"]}}')

        assert get_dependency_versions(local_install=False) == (('1.4.0', ['mq']), '1.0.0')

        capture = capsys.readouterr()
        assert capture.err == ''
        assert capture.out == ''

        requirements_file.unlink()
        requirements_file.write_text('grizzly-loadtester[mq]<=1.5.0')

        requests_mock.register_uri(
            'GET',
            'https://pypi.org/pypi/grizzly-loadtester/json',
            status_code=200,
            text='{"releases": {"1.4.20": [], "1.5.0": [], "1.5.1": []}}',
        )
        requests_mock.register_uri('GET', 'https://pypi.org/pypi/grizzly-loadtester/1.5.0/json', status_code=200, text='{"info": {"requires_dist": ["locust (==1.1.1)"]}}')

        assert get_dependency_versions(local_install=False) == (('1.5.0', ['mq']), '1.1.1')

        capture = capsys.readouterr()
        assert capture.err == ''
        assert capture.out == ''

        local_install_requirements_file = test_context / 'a' / 'b' / 'requirements.txt'
        local_install_requirements_file.parent.mkdir(parents=True)
        local_install_requirements_file.write_text('grizzly-loadtester[mq]>1.3.0,<1.5.0')

        requests_mock.register_uri(
            'GET',
            'https://pypi.org/pypi/grizzly-loadtester/json',
            status_code=200,
            text='{"releases": {"1.3.0": [], "1.4.0": [], "1.5.0": [], "foobar": []}}',
        )
        requests_mock.register_uri('GET', 'https://pypi.org/pypi/grizzly-loadtester/1.4.0/json', status_code=200, text='{"info": {"requires_dist": ["locust (==1.0.0)"]}}')

        assert get_dependency_versions(local_install='a/b/') == (('1.4.0', ['mq']), '1.0.0')
        assert get_dependency_versions(local_install='a/b/requirements.txt') == (('1.4.0', ['mq']), '1.0.0')
    finally:
        rm_rf(test_context)


def test_requirements(capsys: CaptureFixture, tmp_path_factory: TempPathFactory) -> None:
    test_context = tmp_path_factory.mktemp('test_context')
    requirements_file = test_context / 'requirements.txt'

    def wrapped_test(_args: Namespace) -> int:
        return 1337

    try:
        assert not requirements_file.exists()

        wrapped = requirements(test_context.as_posix())(wrapped_test)
        assert getattr(wrapped, '__wrapped__', None) is wrapped_test
        assert getattr(getattr(wrapped, '__wrapped__'), '__value__') == test_context.as_posix()  # noqa: B009

        assert wrapped(Namespace()) == 1337

        capture = capsys.readouterr()
        assert capture.err == ''
        assert capture.out == ''
        assert not requirements_file.exists()

    finally:
        rm_rf(test_context)
