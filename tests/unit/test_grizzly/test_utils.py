"""Unit tests of grizzly.utils."""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from os import environ, utime
from types import FunctionType
from typing import TYPE_CHECKING, Type, cast

import pytest
from locust import TaskSet

from grizzly.context import GrizzlyContext, GrizzlyContextScenario
from grizzly.scenarios import IteratorScenario
from grizzly.tasks import RequestTask
from grizzly.types import RequestMethod
from grizzly.users import GrizzlyUser, RestApiUser
from grizzly.utils import (
    ModuleLoader,
    async_message_request_wrapper,
    check_mq_client_logs,
    create_scenario_class_type,
    create_user_class_type,
    fail_direct,
    flatten,
    has_parameter,
    has_template,
    in_correct_section,
    is_file,
    normalize,
    parse_timespan,
    safe_del,
    unflatten,
)

if TYPE_CHECKING:  # pragma: no cover
    from _pytest.logging import LogCaptureFixture
    from _pytest.tmpdir import TempPathFactory
    from pytest_mock import MockerFixture

    from grizzly.types.behave import Context
    from grizzly_extras.async_message import AsyncMessageRequest
    from tests.fixtures import BehaveFixture, GrizzlyFixture


class TestModuleLoader:
    def test_load_class_non_existent(self) -> None:
        class_name = 'ANonExistingModule'
        default_module = 'grizzly.users'

        with pytest.raises(ModuleNotFoundError, match='No module named'):
            ModuleLoader[GrizzlyUser].load(default_module, f'a.non.existing.package.{class_name}')

        with pytest.raises(AttributeError, match=r"module 'grizzly\.users' has no attribute 'ANonExistingModule'"):
            ModuleLoader[GrizzlyUser].load(default_module, class_name)

    def test_load_user_class(self, behave_fixture: BehaveFixture) -> None:
        try:
            test_context = GrizzlyContext()
            test_context.scenarios.create(behave_fixture.create_scenario('test scenario'))
            test_context.scenario.context['host'] = 'test'
            user_class_name = 'RestApiUser'
            for user_package in ['', 'grizzly.users.', 'grizzly.users.restapi.']:
                user_class_name_value = f'{user_package}{user_class_name}'
                user_class = cast(Type[GrizzlyUser], ModuleLoader[GrizzlyUser].load('grizzly.users', user_class_name_value))  # type: ignore[redundant-cast]
                user_class.__scenario__ = test_context.scenario
                user_class.host = test_context.scenario.context['host']
                assert user_class.__module__ == 'grizzly.users.restapi'
                assert user_class.host == 'test'
                assert hasattr(user_class, 'tasks')

                # try to initialize it, without any token information
                user_class_instance = user_class(behave_fixture.locust.environment)

                # with token context
                test_context.scenario.context['token'] = {
                    'client_secret': 'asdf',
                    'client_id': 'asdf',
                    'url': 'http://test',
                    'resource': None,
                }
                user_class_instance = user_class(behave_fixture.locust.environment)

                # without token context
                test_context.scenario.context['token'] = {
                    'client_secret': None,
                    'client_id': None,
                    'url': None,
                    'resource': None,
                }

                assert type(user_class_instance).__name__ == 'RestApiUser'
                assert user_class_instance.host == 'test'
                assert hasattr(user_class_instance, 'tasks')
        finally:
            GrizzlyContext.destroy()


def test_fail_directly(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    behave.config.stop = False
    behave.config.verbose = True

    with fail_direct(behave):
        assert getattr(behave.config, 'stop', False) is True
        assert getattr(behave.config, 'verbose', True) is False

    assert behave.config.stop is False
    assert behave.config.verbose is True


def test_create_user_class_type(behave_fixture: BehaveFixture) -> None:  # noqa: PLR0915
    scenario = GrizzlyContextScenario(1, behave=behave_fixture.create_scenario('A scenario description'))

    with pytest.raises(ValueError, match='scenario A scenario description does not have a user type set'):
        create_user_class_type(scenario)

    user_orig = scenario.user
    delattr(scenario, 'user')

    with pytest.raises(ValueError, match='scenario A scenario description has not set a user'):
        create_user_class_type(scenario)

    scenario.user = user_orig

    scenario.user.class_name = 'custom.users.CustomUser'
    with pytest.raises(ModuleNotFoundError, match="No module named 'custom'"):
        create_user_class_type(scenario)

    scenario.user.class_name = 'grizzly.users.RestApiUser'
    user_class_type_1 = create_user_class_type(scenario)
    user_class_type_1.host = 'http://localhost:8000'

    assert issubclass(user_class_type_1, (RestApiUser, GrizzlyUser))
    user_class_type_1 = cast(Type[RestApiUser], user_class_type_1)
    assert user_class_type_1.__name__ == f'grizzly.users.RestApiUser_{scenario.identifier}'
    assert user_class_type_1.weight == 1
    assert user_class_type_1.fixed_count == 0
    assert user_class_type_1.sticky_tag is None
    assert user_class_type_1.__scenario__ is scenario
    assert user_class_type_1.host == 'http://localhost:8000'
    assert user_class_type_1.__module__ == 'grizzly.users.restapi'
    assert user_class_type_1.__context__ == {
        'log_all_requests': False,
        'variables': {},
        'verify_certificates': True,
        'auth': {
            'refresh_time': 3000,
            'provider': None,
            'tenant': None,
            'client': {
                'id': None,
                'secret': None,
                'resource': None,
            },
            'user': {
                'username': None,
                'password': None,
                'otp_secret': None,
                'redirect_uri': None,
                'initialize_uri': None,
            },
        },
        'metadata': None,
        '__cached_auth__': {},
        '__context_change_history__': set(),
    }
    user_type_1 = user_class_type_1(behave_fixture.locust.environment)

    assert user_type_1.metadata == {
        'Content-Type': 'application/json',
        'x-grizzly-user': f'grizzly.users.RestApiUser_{scenario.identifier}',
    }

    assert user_type_1.context() == {
        'log_all_requests': False,
        'variables': {},
        'verify_certificates': True,
        'auth': {
            'refresh_time': 3000,
            'provider': None,
            'tenant': None,
            'client': {
                'id': None,
                'secret': None,
                'resource': None,
            },
            'user': {
                'username': None,
                'password': None,
                'otp_secret': None,
                'redirect_uri': None,
                'initialize_uri': None,
            },
        },
        'metadata': {
            'Content-Type': 'application/json',
            'x-grizzly-user': 'grizzly.users.RestApiUser_001',
        },
        '__cached_auth__': {},
        '__context_change_history__': set(),
    }
    assert user_type_1.__scenario__ is scenario

    scenario = GrizzlyContextScenario(1, behave=behave_fixture.create_scenario('TestTestTest'))
    scenario.user.class_name = 'RestApiUser'
    scenario.user.sticky_tag = 'foobar'
    scenario.user.fixed_count = 100
    scenario.context['metadata'] = {
        'Content-Type': 'application/xml',
        'Foo-Bar': 'hello world',
    }
    user_class_type_2 = create_user_class_type(
        scenario,
        {
            'test': {
                'value': 1,
            },
            'log_all_requests': True,
            'auth': {
                'refresh_time': 1337,
                'provider': 'https://auth.example.com',
                'user': {
                    'username': 'grizzly-user',
                },
            },
        },
    )
    user_class_type_2.host = 'http://localhost:8001'

    assert issubclass(user_class_type_2, (RestApiUser, GrizzlyUser))
    user_class_type_2 = cast(Type[RestApiUser], user_class_type_2)
    assert user_class_type_2.__name__ == f'RestApiUser_{scenario.identifier}'
    assert user_class_type_2.weight == 1
    assert user_class_type_2.fixed_count == 100
    assert user_class_type_2.sticky_tag == 'foobar'
    assert user_class_type_2.__scenario__ is scenario
    assert user_class_type_2.host == 'http://localhost:8001'
    assert user_class_type_2.__module__ == 'grizzly.users.restapi'
    assert user_class_type_2.__context__ == {
        'log_all_requests': True,
        'variables': {},
        'test': {
            'value': 1,
        },
        'verify_certificates': True,
        'auth': {
            'refresh_time': 1337,
            'provider': 'https://auth.example.com',
            'tenant': None,
            'client': {
                'id': None,
                'secret': None,
                'resource': None,
            },
            'user': {
                'username': 'grizzly-user',
                'password': None,
                'otp_secret': None,
                'redirect_uri': None,
                'initialize_uri': None,
            },
        },
        'metadata': {
            'Content-Type': 'application/xml',
            'Foo-Bar': 'hello world',
        },
        '__cached_auth__': {},
        '__context_change_history__': set(),
    }

    user_type_2 = user_class_type_2(behave_fixture.locust.environment)
    assert user_type_2.metadata == {
        'Content-Type': 'application/xml',
        'x-grizzly-user': f'RestApiUser_{scenario.identifier}',
        'Foo-Bar': 'hello world',
    }
    assert user_type_2.context() == {
        'log_all_requests': True,
        'variables': {},
        'test': {
            'value': 1,
        },
        'verify_certificates': True,
        'auth': {
            'refresh_time': 1337,
            'provider': 'https://auth.example.com',
            'tenant': None,
            'client': {
                'id': None,
                'secret': None,
                'resource': None,
            },
            'user': {
                'username': 'grizzly-user',
                'password': None,
                'otp_secret': None,
                'redirect_uri': None,
                'initialize_uri': None,
            },
        },
        'metadata': {
            'Content-Type': 'application/xml',
            'Foo-Bar': 'hello world',
            'x-grizzly-user': 'RestApiUser_001',
        },
        '__cached_auth__': {},
        '__context_change_history__': set(),
    }
    assert user_type_2.__scenario__ is scenario

    scenario = GrizzlyContextScenario(1, behave=behave_fixture.create_scenario('TestTestTest2'))
    scenario.user.class_name = 'RestApiUser'
    scenario.context = {'test': {'value': 'hello world', 'description': 'simple text'}}
    user_class_type_3 = create_user_class_type(scenario, {'test': {'value': 1}})
    user_class_type_3.host = 'http://localhost:8002'

    assert issubclass(user_class_type_3, (RestApiUser, GrizzlyUser))
    assert user_class_type_3.__name__ == f'RestApiUser_{scenario.identifier}'
    assert user_class_type_3.weight == 1
    assert user_class_type_3.fixed_count == 0
    assert user_class_type_3.__scenario__ is scenario
    assert user_class_type_3.host == 'http://localhost:8002'
    assert user_class_type_3.__module__ == 'grizzly.users.restapi'
    assert user_class_type_3.__context__ == {
        'log_all_requests': False,
        'variables': {},
        'test': {
            'value': 'hello world',
            'description': 'simple text',
        },
        'verify_certificates': True,
        'auth': {
            'refresh_time': 3000,
            'provider': None,
            'tenant': None,
            'client': {
                'id': None,
                'secret': None,
                'resource': None,
            },
            'user': {
                'username': None,
                'password': None,
                'otp_secret': None,
                'redirect_uri': None,
                'initialize_uri': None,
            },
        },
        'metadata': None,
        '__cached_auth__': {},
        '__context_change_history__': set(),
    }

    assert user_class_type_1.host is not user_class_type_2.host
    assert user_class_type_2.host is not user_class_type_3.host

    user_class_type_3 = create_user_class_type(scenario, {'test': {'value': 1}}, fixed_count=10)
    user_class_type_3.host = 'http://localhost:8002'

    assert issubclass(user_class_type_3, (RestApiUser, GrizzlyUser))
    assert user_class_type_3.__name__ == f'RestApiUser_{scenario.identifier}'
    assert user_class_type_3.weight == 1
    assert user_class_type_3.fixed_count == 10
    assert user_class_type_3.__scenario__ is scenario
    assert user_class_type_3.host == 'http://localhost:8002'
    assert user_class_type_3.__module__ == 'grizzly.users.restapi'
    assert user_class_type_3.__context__ == {
        'log_all_requests': False,
        'variables': {},
        'test': {
            'value': 'hello world',
            'description': 'simple text',
        },
        'verify_certificates': True,
        'auth': {
            'refresh_time': 3000,
            'provider': None,
            'tenant': None,
            'client': {
                'id': None,
                'secret': None,
                'resource': None,
            },
            'user': {
                'username': None,
                'password': None,
                'otp_secret': None,
                'redirect_uri': None,
                'initialize_uri': None,
            },
        },
        'metadata': None,
        '__cached_auth__': {},
        '__context_change_history__': set(),
    }

    scenario = GrizzlyContextScenario(1, behave=behave_fixture.create_scenario('A scenario description'))
    scenario.user.class_name = 'DoNotExistInGrizzlyUsersUser'

    with pytest.raises(AttributeError, match=r"module 'grizzly\.users' has no attribute 'DoNotExistInGrizzlyUsersUser'"):
        create_user_class_type(scenario)


def test_create_scenario_class_type(behave_fixture: BehaveFixture) -> None:
    scenario = GrizzlyContextScenario(1, behave=behave_fixture.create_scenario('A scenario description'))

    with pytest.raises(ModuleNotFoundError, match="No module named 'custom'"):
        create_scenario_class_type('custom.tasks.CustomTasks', scenario)

    task_class_type_1 = create_scenario_class_type('grizzly.scenarios.IteratorScenario', scenario)

    assert issubclass(task_class_type_1, (IteratorScenario, TaskSet))
    assert task_class_type_1.__name__ == 'IteratorScenario_001'
    assert task_class_type_1.__module__ == 'grizzly.scenarios.iterator'
    assert getattr(task_class_type_1, 'pace_time', '') is None
    last_task_1 = task_class_type_1.tasks[-1]
    assert isinstance(last_task_1, FunctionType)
    task_class_type_1.populate(RequestTask(RequestMethod.POST, name='test-request-1', endpoint='/api/test'))
    assert task_class_type_1.tasks[-1] is last_task_1
    assert last_task_1.__name__ == 'pace'

    scenario = GrizzlyContextScenario(2, behave=behave_fixture.create_scenario('TestTestTest'))
    scenario.pace = '2000'
    task_class_type_2 = create_scenario_class_type('IteratorScenario', scenario)
    assert issubclass(task_class_type_2, (IteratorScenario, TaskSet))
    assert task_class_type_2.__name__ == 'IteratorScenario_002'
    assert task_class_type_2.__module__ == 'grizzly.scenarios.iterator'
    assert getattr(task_class_type_2, 'pace_time', '') == '2000'
    last_task_2 = task_class_type_2.tasks[-1]
    assert isinstance(last_task_2, FunctionType)
    task_class_type_2.populate(RequestTask(RequestMethod.POST, name='test-request-2', endpoint='/api/test'))
    assert task_class_type_2.tasks[-1] is last_task_2
    assert last_task_2.__name__ == 'pace'

    assert task_class_type_1.tasks != task_class_type_2.tasks

    scenario = GrizzlyContextScenario(3, behave=behave_fixture.create_scenario('A scenario description'))
    scenario.name = 'A scenario description'

    with pytest.raises(AttributeError, match=r"module 'grizzly\.scenarios' has no attribute 'DoesNotExistInGrizzlyScenariosScenario'"):
        create_scenario_class_type('DoesNotExistInGrizzlyScenariosScenario', scenario)


def test_in_correct_section() -> None:
    from grizzly.steps import step_setup_iterations
    assert in_correct_section(step_setup_iterations, ['grizzly.steps.scenario'])
    assert not in_correct_section(step_setup_iterations, ['grizzly.steps.background'])

    def step_custom(_: Context) -> None:
        pass

    # force AttributeError, for when a step function isn't part of a module
    setattr(step_custom, '__module__', None)  # noqa: B010

    assert in_correct_section(cast(FunctionType, step_custom), ['grizzly.steps.scenario'])


def test_parse_timespan() -> None:
    assert parse_timespan('133') == {'days': 133}
    assert parse_timespan('-133') == {'days': -133}

    with pytest.raises(ValueError, match='invalid time span format'):
        parse_timespan('10P44m')

    with pytest.raises(ValueError, match='invalid time span format'):
        parse_timespan('{}')

    assert parse_timespan('1Y-2M3D-4h5m-6s') == {
        'years': 1,
        'months': -2,
        'days': 3,
        'hours': -4,
        'minutes': 5,
        'seconds': -6,
    }


def test_check_mq_client_logs(behave_fixture: BehaveFixture, tmp_path_factory: TempPathFactory, mocker: MockerFixture, caplog: LogCaptureFixture) -> None:  # noqa: PLR0915
    context = behave_fixture.context
    test_context = tmp_path_factory.mktemp('test_context')

    amq_error_dir = test_context / 'IBM' / 'MQ' / 'data' / 'errors'
    amq_error_dir.mkdir(parents=True, exist_ok=True)

    test_logger = logging.getLogger('test_grizzly_print_stats')

    mocker.patch('grizzly.utils.Path.expanduser', return_value=amq_error_dir)
    mocker.patch('grizzly.locust.stats_logger', test_logger)

    # context.started not set
    with caplog.at_level(logging.INFO):
        check_mq_client_logs(context)

    assert len(caplog.messages) == 0

    # no error files
    context.started = datetime.now().astimezone()
    with caplog.at_level(logging.INFO):
        check_mq_client_logs(context)

    assert len(caplog.messages) == 0

    # one AMQERR*.LOG file, previous run
    amqerr_log_file_1 = amq_error_dir / 'AMQERR01.LOG'
    amqerr_log_file_1.write_text("""10/13/22 06:13:07 - Process(6437.52) User(mqm) Program(amqrmppa)
                    Host(mq.example.io) Installation(Installation1)
                    VRMF(9.2.1.0) QMgr(QM1)
                    Time(2022-10-13T06:13:07.215Z)
                    CommentInsert1(CLIENT.CONN)
                    CommentInsert2(1111)
                    CommentInsert3(1.2.3.4)

AMQ9999E: Channel 'CLIENT.CONN' to host '1.2.3.4' ended abnormally.

EXPLANATION:
----- amqccisa.c : 10957 ------------------------------------------------------
""")

    with caplog.at_level(logging.INFO):
        check_mq_client_logs(context)

    assert len(caplog.messages) == 0

    entry_date_1 = (datetime.now() + timedelta(hours=1)).astimezone(tz=timezone.utc)

    # one AMQERR*.LOG file, one entry
    with amqerr_log_file_1.open('a') as fd:
        fd.write(f"""{entry_date_1.strftime('%m/%d/%y %H:%M:%S')} - Process(6437.52) User(mqm) Program(amqrmppa)
                    Host(mq.example.io) Installation(Installation1)
                    VRMF(9.2.1.0) QMgr(QM1)
                    Time({entry_date_1.strftime('%Y-%m-%dT%H:%M:%S.000Z')})
                    CommentInsert1(CLIENT.CONN)
                    CommentInsert2(1111)
                    CommentInsert3(1.2.3.4)

AMQ9999E: Channel 'CLIENT.CONN' to host '1.2.3.4' ended abnormally.

EXPLANATION:
----- amqccisa.c : 10957 ------------------------------------------------------
""")

    with caplog.at_level(logging.INFO):
        check_mq_client_logs(context)

    assert len(caplog.messages) == 6

    assert caplog.messages[0] == 'AMQ error log entries:'
    assert caplog.messages[1].strip() == 'Timestamp (UTC)      Message'
    assert caplog.messages[3].strip() == f"{entry_date_1.strftime('%Y-%m-%d %H:%M:%S')}  AMQ9999E: Channel 'CLIENT.CONN' to host '1.2.3.4' ended abnormally."
    assert (
        caplog.messages[2]
        == caplog.messages[4]
        == (
            '--------------------|-----------------------------------------------------------------'
            '----------------------------------------------------------------------------'
        )
    )
    assert caplog.messages[5] == ''

    caplog.clear()

    # two AMQERR files, one with no data, one FDC file, old
    old_date = entry_date_1 - timedelta(hours=2)

    amqerr_log_file_2 = amq_error_dir / 'AMQERR99.LOG'
    amqerr_log_file_2.touch()

    amqerr_fdc_file_1 = amq_error_dir / 'AMQ6150.0.FDC'
    amqerr_fdc_file_1.touch()
    utime(amqerr_fdc_file_1, (old_date.timestamp(), old_date.timestamp()))

    with caplog.at_level(logging.INFO):
        check_mq_client_logs(context)

    assert len(caplog.messages) == 6

    assert caplog.messages[0] == 'AMQ error log entries:'
    assert caplog.messages[1].strip() == 'Timestamp (UTC)      Message'
    assert caplog.messages[3].strip() == f"{entry_date_1.strftime('%Y-%m-%d %H:%M:%S')}  AMQ9999E: Channel 'CLIENT.CONN' to host '1.2.3.4' ended abnormally."
    assert (
        caplog.messages[2]
        == caplog.messages[4]
        == (
            '--------------------|-----------------------------------------------------------------'
            '----------------------------------------------------------------------------'
        )
    )
    assert caplog.messages[5] == ''

    caplog.clear()

    # two AMQERR files, both with valid data. three FDC files, one old
    entry_date_2 = entry_date_1 + timedelta(minutes=23)
    amqerr_log_file_2.write_text(f"""{entry_date_2.strftime('%m/%d/%y %H:%M:%S')} - Process(6437.52) User(mqm) Program(amqrmppa)
                    Host(mq.example.io) Installation(Installation1)
                    VRMF(9.2.1.0) QMgr(QM1)
                    Time({entry_date_2.strftime('%Y-%m-%dT%H:%M:%S.000Z')})
                    CommentInsert1(CLIENT.CONN)
                    CommentInsert2(1111)
                    CommentInsert3(1.2.3.4)

AMQ1234E: dude, what did you do?!

EXPLANATION:
----- amqccisa.c : 10957 ------------------------------------------------------
""")
    amqerr_fdc_file_2 = amq_error_dir / 'AMQ1234.1.FDC'
    amqerr_fdc_file_2.touch()
    utime(amqerr_fdc_file_2, (entry_date_2.timestamp(), entry_date_2.timestamp()))

    amqerr_fdc_file_3 = amq_error_dir / 'AMQ4321.9.FDC'
    amqerr_fdc_file_3.touch()
    entry_date_3 = entry_date_2 + timedelta(minutes=73)
    utime(amqerr_fdc_file_3, (entry_date_3.timestamp(), entry_date_3.timestamp()))

    with caplog.at_level(logging.INFO):
        check_mq_client_logs(context)

    assert len(caplog.messages) == 14

    assert caplog.messages.index('AMQ error log entries:') == 0
    assert caplog.messages.index('AMQ FDC files:') == 7

    amqerr_log_entries = caplog.messages[0:6]
    assert len(amqerr_log_entries) == 6

    assert amqerr_log_entries[1].strip() == 'Timestamp (UTC)      Message'
    assert (
        amqerr_log_entries[2]
        == amqerr_log_entries[5]
        == (
            '--------------------|-----------------------------------------------------------------'
            '----------------------------------------------------------------------------'
        )
    )
    assert amqerr_log_entries[3].strip() == f"{entry_date_1.strftime('%Y-%m-%d %H:%M:%S')}  AMQ9999E: Channel 'CLIENT.CONN' to host '1.2.3.4' ended abnormally."
    assert amqerr_log_entries[4].strip() == f'{entry_date_2.strftime("%Y-%m-%d %H:%M:%S")}  AMQ1234E: dude, what did you do?!'

    amqerr_fdc_files = caplog.messages[7:-1]
    assert len(amqerr_fdc_files) == 6

    assert amqerr_fdc_files[1].strip() == 'Timestamp (UTC)      File'
    assert (
        amqerr_fdc_files[2]
        == amqerr_fdc_files[5]
        == (
            '--------------------|-----------------------------------------------------------------'
            '----------------------------------------------------------------------------'
        )
    )

    assert amqerr_fdc_files[3].strip() == f'{entry_date_2.strftime("%Y-%m-%d %H:%M:%S")}  {amqerr_fdc_file_2}'
    assert amqerr_fdc_files[4].strip() == f'{entry_date_3.strftime("%Y-%m-%d %H:%M:%S")}  {amqerr_fdc_file_3}'


def test_async_message_request_wrapper(grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:
    parent = grizzly_fixture()

    async_message_request_mock = mocker.patch('grizzly.utils.async_message_request', return_value=None)
    client_mock = mocker.MagicMock()

    # nothing to render
    request: AsyncMessageRequest = {
        'context': {
            'endpoint': 'hello world',
        },
    }

    async_message_request_wrapper(parent, client_mock, request)

    request.update({'client': id(parent.user)})

    async_message_request_mock.assert_called_once_with(client_mock, request)
    async_message_request_mock.reset_mock()

    del request['client']

    # template to render, variable not set
    request = {
        'context': {
            'endpoint': 'hello {{ world }}!',
        },
    }

    async_message_request_wrapper(parent, client_mock, request)

    async_message_request_mock.assert_called_once_with(client_mock, {'context': {'endpoint': 'hello !'}, 'client': id(parent.user)})
    async_message_request_mock.reset_mock()

    # template to render, variable set
    parent.user._context['variables'].update({'world': 'foobar'})

    async_message_request_wrapper(parent, client_mock, request)

    async_message_request_mock.assert_called_once_with(client_mock, {'context': {'endpoint': 'hello foobar!'}, 'client': id(parent.user)})


def test_safe_del() -> None:
    struct = {'hello': 'world', 'foo': 'bar'}

    safe_del(struct, 'bar')
    assert struct == {'hello': 'world', 'foo': 'bar'}

    safe_del(struct, 'hello')
    assert struct == {'foo': 'bar'}

    safe_del(struct, 'foo')
    assert struct == {}

    safe_del(struct, 'hello')
    assert struct == {}


def test_has_template() -> None:
    assert has_template('{{ hello_world }}')
    assert not has_template('{{ hello_world')
    assert not has_template('hello_world }}')
    assert has_template('is {{ this }} really a template?')


def test_has_parameter() -> None:
    assert has_parameter('hello $conf::world$!')
    assert not has_parameter('hello $conf::world!')
    assert has_parameter('$conf::foo$ and then $conf::bar$ and even $env::HELLO_WORLD$!')
    assert not has_parameter('queue:test-queue')
    assert not has_parameter('hello world')
    assert not has_parameter('$hello::')


def test_is_file(behave_fixture: BehaveFixture) -> None:
    original = environ.get('GRIZZLY_CONTEXT_ROOT', None)

    try:
        assert not is_file('')
        assert not is_file('test/input.csv')

        test_file = behave_fixture.locust._test_context_root / 'requests' / 'test' / 'input.csv'
        test_file.parent.mkdir(parents=True, exist_ok=True)
        test_file.touch()

        assert is_file('test/input.csv')

        del environ['GRIZZLY_CONTEXT_ROOT']

        assert not is_file('test/input.csv')

        if original is not None:
            environ['GRIZZLY_CONTEXT_ROOT'] = original

        text = """Lorem ipsum dolor sit amet, consectetur adipiscing elit. Donec orci urna, pulvinar ut diam ac, egestas ultricies urna. Quisque blandit fermentum rutrum.
In vulputate nibh non nibh tempor, scelerisque feugiat libero accumsan. Pellentesque semper vestibulum diam a dictum. Integer ac nunc quis lorem tristique scelerisque
id vitae mi. Integer mollis nisi dolor, a pulvinar erat sollicitudin viverra. Ut mattis metus arcu, ut finibus ligula sollicitudin vitae. Nulla ornare sem eget libero
tincidunt consectetur. Pellentesque nec fermentum tortor, et scelerisque tortor.

Sed id risus convallis, ornare diam quis, convallis urna. Suspendisse vel pharetra ipsum, consectetur fringilla enim. Pellentesque non libero sit amet quam suscipit sagittis
ac vitae lorem. Nullam venenatis sit amet lacus nec viverra. Sed venenatis ullamcorper diam at rhoncus. Ut imperdiet nec ipsum eget vehicula. Etiam pellentesque tempor commodo.
Nullam elementum lorem ac sem finibus, in maximus nibh molestie. Quisque iaculis ipsum placerat magna aliquam, nec tempor nisl consequat. Curabitur aliquam elit libero, et
posuere ante ultricies vitae. Aliquam erat volutpat.

Nullam sagittis quam ut tellus fermentum, nec eleifend arcu facilisis. Donec sit amet ante et nulla pellentesque suscipit. Pellentesque quam metus, luctus non lorem rutrum,
faucibus tempus ex. Vestibulum maximus, sem eu facilisis elementum, elit leo suscipit urna, at auctor ipsum ex ut mi. Nulla imperdiet ligula eu finibus ornare. Aenean tincidunt
eu magna maximus varius. Nam eu libero justo. In rhoncus nulla nisl, non mollis sem luctus ut. Pellentesque tempor rutrum nulla, at imperdiet lorem maximus sit amet.

Nunc metus massa, laoreet sit amet aliquam eu, ultrices in justo. Maecenas congue lectus felis, eu pellentesque nisl finibus et. Nulla suscipit, lacus ut tincidunt cursus,
leo eros tristique lacus, aliquet sollicitudin ante ipsum ut mauris. Duis porttitor dapibus pulvinar. Suspendisse viverra, felis nec suscipit convallis, augue purus accumsan dui,
eget varius nulla mi sed orci. Vestibulum sit amet risus egestas, rhoncus nibh id, bibendum tortor. Proin auctor magna odio.

Quisque in ultricies quam, et mattis arcu. Class aptent taciti sociosqu ad litora torquent per conubia nostra, per inceptos himenaeos. Vestibulum fringilla velit non sollicitudin
vestibulum. Curabitur luctus nisi eget nulla fringilla, sit amet feugiat ipsum imperdiet. Nunc tristique scelerisque ligula quis hendrerit. Fusce ac interdum nisi. Phasellus
interdum, justo sit amet convallis dictum, enim urna viverra tortor, vel viverra odio arcu vitae massa. Sed mattis vehicula dui quis mattis. Vestibulum vehicula tincidunt lobortis.
Mauris bibendum nulla orci, nec viverra tellus semper nec. Nulla vel nisl luctus, venenatis purus eget, iaculis neque. Maecenas venenatis dui quis varius interdum. Nullam finibus
consequat augue et pretium. Aliquam eget finibus eros.
"""
        assert not is_file(text)

        assert not is_file('foobar')

    finally:
        if original is not None:
            environ['GRIZZLY_CONTEXT_ROOT'] = original


def test_normalize() -> None:
    assert normalize('test') == 'test'
    assert normalize('Hello World!') == 'Hello-World'
    assert normalize('[does]this-look* <strange>!') == 'doesthis-look-strange'


def test_flatten() -> None:
    assert flatten({
        'hello': {'world': {'foo': {'bar': 'foobar'}}},
        'foo': {'bar': 'hello world!'},
    }) == {
        'hello.world.foo.bar': 'foobar',
        'foo.bar': 'hello world!',
    }


def test_unflatten() -> None:
    assert unflatten('hello.world.foo.bar', 'foobar') == {
        'hello': {'world': {'foo': {'bar': 'foobar'}}},
    }

    assert unflatten('foo.bar', 'hello world!') == {'foo': {'bar': 'hello world!'}}
