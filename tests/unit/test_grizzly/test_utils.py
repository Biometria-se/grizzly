import logging

from typing import Type, cast
from types import FunctionType
from datetime import datetime, timedelta, timezone
from os import utime

import pytest

from _pytest.tmpdir import TempPathFactory
from _pytest.logging import LogCaptureFixture
from pytest_mock import MockerFixture

from locust import TaskSet

from grizzly.utils import ModuleLoader
from grizzly.utils import (
    create_scenario_class_type,
    create_user_class_type,
    fail_direct,
    in_correct_section,
    parse_timespan,
    check_mq_client_logs,
    async_message_request_wrapper,
    safe_del,
)
from grizzly.types import RequestMethod
from grizzly.types.behave import Context
from grizzly.context import GrizzlyContext, GrizzlyContextScenario
from grizzly.tasks import RequestTask
from grizzly.users import RestApiUser
from grizzly.users.base import GrizzlyUser
from grizzly.scenarios import IteratorScenario
from grizzly_extras.async_message import AsyncMessageRequest

from tests.fixtures import BehaveFixture, GrizzlyFixture


class TestModuleLoader:
    def test_load_class_non_existent(self) -> None:
        class_name = 'ANonExistingModule'

        with pytest.raises(ModuleNotFoundError):
            ModuleLoader[GrizzlyUser].load('a.non.existing.package', class_name)

        with pytest.raises(AttributeError):
            ModuleLoader[GrizzlyUser].load('grizzly.users', class_name)

    def test_load_user_class(self, behave_fixture: BehaveFixture) -> None:
        try:
            test_context = GrizzlyContext()
            test_context.scenarios.create(behave_fixture.create_scenario('test scenario'))
            test_context.scenario.context['host'] = 'test'
            user_class_name = 'RestApiUser'
            for user_package in ['', 'grizzly.users.', 'grizzly.users.restapi.']:
                user_class_name_value = f'{user_package}{user_class_name}'
                user_class = cast(Type[GrizzlyUser], ModuleLoader[GrizzlyUser].load('grizzly.users', user_class_name_value))
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


def test_create_user_class_type(behave_fixture: BehaveFixture) -> None:
    scenario = GrizzlyContextScenario(1, behave=behave_fixture.create_scenario('A scenario description'))

    with pytest.raises(ValueError) as ve:
        create_user_class_type(scenario)
    assert 'scenario A scenario description does not have a user type set' in str(ve)

    user_orig = scenario.user
    delattr(scenario, 'user')

    with pytest.raises(ValueError) as ve:
        create_user_class_type(scenario)
    assert 'scenario A scenario description has not set a user' in str(ve)

    setattr(scenario, 'user', user_orig)

    scenario.user.class_name = 'custom.users.CustomUser'
    with pytest.raises(ModuleNotFoundError) as mnfe:
        create_user_class_type(scenario)
    assert "No module named 'custom'" in str(mnfe)

    scenario.user.class_name = 'grizzly.users.RestApiUser'
    user_class_type_1 = create_user_class_type(scenario)
    user_class_type_1.host = 'http://localhost:8000'

    assert issubclass(user_class_type_1, (RestApiUser, GrizzlyUser))
    user_class_type_1 = cast(Type[RestApiUser], user_class_type_1)
    assert user_class_type_1.__name__ == f'grizzly.users.RestApiUser_{scenario.identifier}'
    assert user_class_type_1.weight == 1
    assert user_class_type_1.__scenario__ is scenario
    assert user_class_type_1.host == 'http://localhost:8000'
    assert user_class_type_1.__module__ == 'grizzly.users.restapi'
    assert user_class_type_1._context == {
        'verify_certificates': True,
        'auth': {
            'refresh_time': 3000,
            'provider': None,
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
    }
    user_type_1 = user_class_type_1(behave_fixture.locust.environment)

    assert user_type_1.headers == {
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
    }
    assert user_type_1.__scenario__ is scenario

    scenario = GrizzlyContextScenario(1, behave=behave_fixture.create_scenario('TestTestTest'))
    scenario.user.class_name = 'RestApiUser'
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
                }
            },
        }
    )
    user_class_type_2.host = 'http://localhost:8001'

    assert issubclass(user_class_type_2, (RestApiUser, GrizzlyUser))
    user_class_type_2 = cast(Type[RestApiUser], user_class_type_2)
    assert user_class_type_2.__name__ == f'RestApiUser_{scenario.identifier}'
    assert user_class_type_2.weight == 1
    assert user_class_type_2.__scenario__ is scenario
    assert user_class_type_2.host == 'http://localhost:8001'
    assert user_class_type_2.__module__ == 'grizzly.users.restapi'
    assert user_class_type_2._context == {
        'log_all_requests': True,
        'test': {
            'value': 1,
        },
        'verify_certificates': True,
        'auth': {
            'refresh_time': 1337,
            'provider': 'https://auth.example.com',
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
    }

    user_type_2 = user_class_type_2(behave_fixture.locust.environment)
    assert user_type_2.headers == {
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
    assert user_class_type_3._context == {
        'test': {
            'value': 'hello world',
            'description': 'simple text',
        },
        'verify_certificates': True,
        'auth': {
            'refresh_time': 3000,
            'provider': None,
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
    assert user_class_type_3._context == {
        'test': {
            'value': 'hello world',
            'description': 'simple text',
        },
        'verify_certificates': True,
        'auth': {
            'refresh_time': 3000,
            'provider': None,
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
    }

    with pytest.raises(AttributeError):
        scenario = GrizzlyContextScenario(1, behave=behave_fixture.create_scenario('A scenario description'))
        scenario.user.class_name = 'DoNotExistInGrizzlyUsersUser'
        create_user_class_type(scenario)


def test_create_scenario_class_type(behave_fixture: BehaveFixture) -> None:
    scenario = GrizzlyContextScenario(1, behave=behave_fixture.create_scenario('A scenario description'))

    with pytest.raises(ModuleNotFoundError) as mnfe:
        create_scenario_class_type('custom.tasks.CustomTasks', scenario)
    assert "No module named 'custom'" in str(mnfe)

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

    with pytest.raises(AttributeError):
        scenario = GrizzlyContextScenario(3, behave=behave_fixture.create_scenario('A scenario description'))
        scenario.name = 'A scenario description'
        create_scenario_class_type('DoesNotExistInGrizzlyScenariosModel', scenario)


def test_in_correct_section() -> None:
    from grizzly.steps import step_setup_iterations
    assert in_correct_section(step_setup_iterations, ['grizzly.steps.scenario'])
    assert not in_correct_section(step_setup_iterations, ['grizzly.steps.background'])

    def step_custom(context: Context) -> None:
        pass

    # force AttributeError, for when a step function isn't part of a module
    setattr(step_custom, '__module__', None)

    assert in_correct_section(cast(FunctionType, step_custom), ['grizzly.steps.scenario'])


def test_parse_timespan() -> None:
    assert parse_timespan('133') == {'days': 133}
    assert parse_timespan('-133') == {'days': -133}

    with pytest.raises(ValueError) as ve:
        parse_timespan('10P44m')
    assert 'invalid time span format' in str(ve)

    with pytest.raises(ValueError) as ve:
        parse_timespan('{}')
    assert 'invalid time span format' in str(ve)

    assert parse_timespan('1Y-2M3D-4h5m-6s') == {
        'years': 1,
        'months': -2,
        'days': 3,
        'hours': -4,
        'minutes': 5,
        'seconds': -6,
    }


def test_check_mq_client_logs(behave_fixture: BehaveFixture, tmp_path_factory: TempPathFactory, mocker: MockerFixture, caplog: LogCaptureFixture) -> None:
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
    amqerr_log_file_1.write_text('''10/13/22 06:13:07 - Process(6437.52) User(mqm) Program(amqrmppa)
                    Host(mq.example.io) Installation(Installation1)
                    VRMF(9.2.1.0) QMgr(QM1)
                    Time(2022-10-13T06:13:07.215Z)
                    CommentInsert1(CLIENT.CONN)
                    CommentInsert2(1111)
                    CommentInsert3(1.2.3.4)

AMQ9999E: Channel 'CLIENT.CONN' to host '1.2.3.4' ended abnormally.

EXPLANATION:
----- amqccisa.c : 10957 ------------------------------------------------------
''')

    with caplog.at_level(logging.INFO):
        check_mq_client_logs(context)

    assert len(caplog.messages) == 0

    entry_date_1 = (datetime.now() + timedelta(hours=1)).astimezone(tz=timezone.utc)

    # one AMQERR*.LOG file, one entry
    with amqerr_log_file_1.open('a') as fd:
        fd.write(f'''{entry_date_1.strftime('%m/%d/%y %H:%M:%S')} - Process(6437.52) User(mqm) Program(amqrmppa)
                    Host(mq.example.io) Installation(Installation1)
                    VRMF(9.2.1.0) QMgr(QM1)
                    Time({entry_date_1.strftime('%Y-%m-%dT%H:%M:%S.000Z')})
                    CommentInsert1(CLIENT.CONN)
                    CommentInsert2(1111)
                    CommentInsert3(1.2.3.4)

AMQ9999E: Channel 'CLIENT.CONN' to host '1.2.3.4' ended abnormally.

EXPLANATION:
----- amqccisa.c : 10957 ------------------------------------------------------
''')

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
    amqerr_log_file_2.write_text(f'''{entry_date_2.strftime('%m/%d/%y %H:%M:%S')} - Process(6437.52) User(mqm) Program(amqrmppa)
                    Host(mq.example.io) Installation(Installation1)
                    VRMF(9.2.1.0) QMgr(QM1)
                    Time({entry_date_2.strftime('%Y-%m-%dT%H:%M:%S.000Z')})
                    CommentInsert1(CLIENT.CONN)
                    CommentInsert2(1111)
                    CommentInsert3(1.2.3.4)

AMQ1234E: dude, what did you do?!

EXPLANATION:
----- amqccisa.c : 10957 ------------------------------------------------------
''')
    amqerr_fdc_file_2 = amq_error_dir / 'AMQ1234.1.FDC'
    amqerr_fdc_file_2.touch()
    utime(amqerr_fdc_file_2, (entry_date_2.timestamp(), entry_date_2.timestamp(),))

    amqerr_fdc_file_3 = amq_error_dir / 'AMQ4321.9.FDC'
    amqerr_fdc_file_3.touch()
    entry_date_3 = entry_date_2 + timedelta(minutes=73)
    utime(amqerr_fdc_file_3, (entry_date_3.timestamp(), entry_date_3.timestamp(),))

    with caplog.at_level(logging.INFO):
        check_mq_client_logs(context)

    print('\n'.join(caplog.messages))

    assert len(caplog.messages) == 14

    assert caplog.messages.index('AMQ error log entries:') == 0
    assert caplog.messages.index('AMQ FDC files:') == 7

    amqerr_log_entries = caplog.messages[0:6]
    print(amqerr_log_entries)
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
            'endpoint': 'hello world'
        }
    }

    async_message_request_wrapper(parent, client_mock, request)

    request.update({'client': id(parent.user)})

    async_message_request_mock.assert_called_once_with(client_mock, request)
    async_message_request_mock.reset_mock()

    del request['client']

    # template to render, variable not set
    request = {
        'context': {
            'endpoint': 'hello {{ world }}!'
        }
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
