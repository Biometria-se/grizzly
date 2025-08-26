from __future__ import annotations

import logging
import sys
from argparse import Namespace
from typing import TYPE_CHECKING

import pytest
from grizzly_ls.__main__ import main, parse_arguments, setup_debugging, setup_logging

if TYPE_CHECKING:
    from _pytest.capture import CaptureFixture
    from pytest_mock import MockerFixture


def test_parse_arguments(capsys: CaptureFixture[str]) -> None:
    sys.argv = ['grizzly-ls']

    args = parse_arguments()

    assert args == Namespace(
        socket=False,
        socket_port=4444,
        verbose=False,
        version=False,
        no_verbose=None,
        command=None,
        debug=False,
        debug_port=5678,
        debug_wait=False,
        embedded=False,
    )

    sys.argv = [
        'grizzly-ls',
        '--socket',
        '--socket-port',
        '5555',
        '--no-verbose',
        'pygls',
        'behave',
        '--verbose',
    ]

    args = parse_arguments()

    assert args == Namespace(
        socket=True,
        socket_port=5555,
        verbose=True,
        version=False,
        no_verbose=['pygls', 'behave'],
        command=None,
        debug=False,
        debug_port=5678,
        debug_wait=False,
        embedded=False,
    )

    sys.argv = ['grizzly-ls', '--version']

    with pytest.raises(SystemExit) as se:
        parse_arguments()
    assert se.value.code == 0

    capture = capsys.readouterr()

    assert capture.out == ''
    assert capture.err != ''

    sys.argv = ['grizzly-ls', 'lint']

    with pytest.raises(SystemExit) as se:
        parse_arguments()
    assert se.value.code == 2

    sys.argv = ['grizzly-ls', 'lint', '.']

    args = parse_arguments()

    assert args == Namespace(
        socket=False,
        socket_port=4444,
        verbose=False,
        version=False,
        no_verbose=None,
        command='lint',
        files=['.'],
        debug=False,
        debug_port=5678,
        debug_wait=False,
        embedded=False,
    )

    sys.argv = [
        'grizzly-ls',
        '--debug',
        '--debug-port=1234',
        '--debug-wait',
        '--embedded',
        'lint',
        '.',
    ]

    args = parse_arguments()

    assert args == Namespace(
        socket=False,
        socket_port=4444,
        verbose=False,
        version=False,
        no_verbose=None,
        command='lint',
        files=['.'],
        debug=True,
        debug_port=1234,
        debug_wait=True,
        embedded=True,
    )


def test_setup_logging(mocker: MockerFixture, capsys: CaptureFixture[str]) -> None:  # noqa: PLR0915
    logging_basic_config_mock = mocker.patch('grizzly_ls.__main__.logging.basicConfig')
    logging_file_handler_mock = mocker.patch('grizzly_ls.__main__.logging.FileHandler', spec_set=logging.FileHandler)
    logging_stream_handler_mock = mocker.patch('grizzly_ls.__main__.logging.StreamHandler', spec_set=logging.StreamHandler)

    # <no args>
    arguments = Namespace(socket=False, verbose=False, no_verbose=None, embedded=False)

    setup_logging(arguments)

    assert logging_basic_config_mock.call_count == 1
    _, kwargs = logging_basic_config_mock.call_args_list[-1]
    assert kwargs.get('level', None) == logging.INFO
    assert kwargs.get('format', None) is None
    handlers = kwargs.get('handlers', None)
    assert len(handlers) == 1
    assert logging_file_handler_mock.call_count == 0
    assert logging_stream_handler_mock.call_count == 1
    capture = capsys.readouterr()
    assert capture.err == ''
    assert capture.out == ''

    logging_stream_handler_mock.reset_mock()

    # --verbose, --no-verbose pygls behave
    arguments = Namespace(socket=False, verbose=True, no_verbose=['pygls', 'behave'], embedded=False)

    setup_logging(arguments)

    assert logging_basic_config_mock.call_count == 2
    _, kwargs = logging_basic_config_mock.call_args_list[-1]
    assert kwargs.get('level', None) == logging.DEBUG
    assert kwargs.get('format', None) is None
    handlers = kwargs.get('handlers', None)
    assert len(handlers) == 2
    assert logging_stream_handler_mock.call_count == 1
    assert logging_file_handler_mock.call_count == 1
    args, _ = logging_file_handler_mock.call_args_list[-1]
    assert args[0] == 'grizzly-ls.log'
    assert logging.getLogger('pygls').getEffectiveLevel() == logging.ERROR
    assert logging.getLogger('parse').getEffectiveLevel() == logging.ERROR
    capture = capsys.readouterr()
    assert capture.err == ''
    assert capture.out == ''

    logging_stream_handler_mock.reset_mock()

    # --socket
    arguments = Namespace(socket=True, verbose=False, no_verbose=None, embedded=False)

    setup_logging(arguments)

    assert logging_basic_config_mock.call_count == 3
    _, kwargs = logging_basic_config_mock.call_args_list[-1]
    assert kwargs.get('level', None) == logging.INFO
    assert kwargs.get('format', None) is None
    handlers = kwargs.get('handlers', None)
    assert len(handlers) == 1
    assert logging_stream_handler_mock.call_count == 1
    args, _ = logging_stream_handler_mock.call_args_list[-1]
    assert args[0] is sys.stderr
    assert logging_file_handler_mock.call_count == 1
    capture = capsys.readouterr()
    assert capture.err == ''
    assert capture.out == ''

    # --embedded
    arguments = Namespace(socket=True, verbose=False, no_verbose=None, embedded=True)

    setup_logging(arguments)

    assert logging_basic_config_mock.call_count == 4
    _, kwargs = logging_basic_config_mock.call_args_list[-1]
    assert kwargs.get('level', None) == logging.INFO
    assert kwargs.get('format', None) is None
    handlers = kwargs.get('handlers', None)
    assert len(handlers) == 0  # no Stream or File handler
    assert logging_stream_handler_mock.call_count == 1


def test_setup_debugging(mocker: MockerFixture) -> None:
    mocker.patch('grizzly_ls.__main__.logging.info')

    # enable debugging with missing debugpy module
    prev_path = sys.path
    try:
        if 'debugpy' in sys.modules:
            del sys.modules['debugpy']
        sys.path = []

        arguments = Namespace(
            socket=False,
            verbose=False,
            no_verbose=None,
            debug=True,
            debug_port=5678,
            debug_wait=False,
        )
        err_msg = setup_debugging(arguments)
        assert err_msg == 'debugging requires the debugpy package to be installed'
    finally:
        sys.path = prev_path

    try:
        debugpy_listen_mock = mocker.patch('debugpy.listen')
        debugby_wait_for_client_mock = mocker.patch('debugpy.wait_for_client')

        # <no args>
        arguments = Namespace(
            socket=False,
            verbose=False,
            no_verbose=None,
            debug=False,
            debug_port=5678,
            debug_wait=False,
        )

        setup_debugging(arguments)
        debugpy_listen_mock.assert_not_called()
        debugby_wait_for_client_mock.assert_not_called()
        debugpy_listen_mock.reset_mock()
        debugby_wait_for_client_mock.reset_mock()

        # debug enabled
        arguments = Namespace(
            socket=False,
            verbose=False,
            no_verbose=None,
            debug=True,
            debug_port=5678,
            debug_wait=False,
        )

        setup_debugging(arguments)
        debugpy_listen_mock.assert_called_once_with(5678)
        debugby_wait_for_client_mock.assert_not_called()
        debugpy_listen_mock.reset_mock()
        debugby_wait_for_client_mock.reset_mock()

        # debug enabled + wait for client
        arguments = Namespace(
            socket=False,
            verbose=False,
            no_verbose=None,
            debug=True,
            debug_port=6789,
            debug_wait=True,
        )

        setup_debugging(arguments)
        debugpy_listen_mock.assert_called_once_with(6789)
        debugby_wait_for_client_mock.assert_called_once()
    except ModuleNotFoundError:
        pytest.skip(reason='grizzly-ls not installed with extra group "debug"')


def test_main(mocker: MockerFixture) -> None:
    mocker.patch('grizzly_ls.__main__.setup_logging', return_value=None)  # no logging in test
    mocker.patch('grizzly_ls.__main__.setup_debugging', return_value=None)  # no debugging in test

    server_start_io_mock = mocker.patch('grizzly_ls.server.server.start_io', return_value=None)
    server_start_tcp_mock = mocker.patch('grizzly_ls.server.server.start_tcp', return_value=None)

    # <no args>
    sys.argv = ['grizzly-ls']

    main()

    server_start_io_mock.assert_called_once_with(sys.stdin.buffer, sys.stdout.buffer)
    server_start_tcp_mock.assert_not_called()
    server_start_io_mock.reset_mock()

    # --socket
    sys.argv = ['grizzly-ls', '--socket']

    main()

    server_start_io_mock.assert_not_called()
    server_start_tcp_mock.assert_called_once_with('127.0.0.1', 4444)

    # if start_debugging returns an error message, it should be added to the server
    mocker.patch('grizzly_ls.__main__.setup_debugging', return_value='some error')
    server_add_startup_error_message = mocker.patch('grizzly_ls.server.server.add_startup_error_message', return_value=None)
    sys.argv = ['grizzly-ls', '--debug']

    main()

    server_add_startup_error_message.assert_called_once_with('some error')
