"""Unit tests of grizzly.utils."""

from __future__ import annotations

from os import environ
from types import FunctionType
from typing import TYPE_CHECKING, cast

import pytest
from grizzly.context import GrizzlyContextScenario
from grizzly.scenarios import IteratorScenario
from grizzly.tasks import RequestTask
from grizzly.types import RequestMethod
from grizzly.users import GrizzlyUser, RestApiUser
from grizzly.utils import (
    ModuleLoader,
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
from locust import TaskSet

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.types.behave import Context

    from test_framework.fixtures import BehaveFixture


class TestModuleLoader:
    def test_load_class_non_existent(self) -> None:
        class_name = 'ANonExistingModule'
        default_module = 'grizzly.users'

        with pytest.raises(ModuleNotFoundError, match='No module named'):
            ModuleLoader[GrizzlyUser].load(default_module, f'a.non.existing.package.{class_name}')

        with pytest.raises(AttributeError, match=r"module 'grizzly\.users' has no attribute 'ANonExistingModule'"):
            ModuleLoader[GrizzlyUser].load(default_module, class_name)

    def test_load_user_class(self, behave_fixture: BehaveFixture) -> None:
        test_context = behave_fixture.grizzly
        test_context.scenarios.create(behave_fixture.create_scenario('test scenario'))
        test_context.scenario.context['host'] = 'test'
        user_class_name = 'RestApiUser'
        for user_package in ['', 'grizzly.users.', 'grizzly.users.restapi.']:
            user_class_name_value = f'{user_package}{user_class_name}'
            user_class = cast('type[GrizzlyUser]', ModuleLoader[GrizzlyUser].load('grizzly.users', user_class_name_value))  # type: ignore[redundant-cast]
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
    scenario = GrizzlyContextScenario(1, behave=behave_fixture.create_scenario('A scenario description'), grizzly=behave_fixture.grizzly)

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

    assert issubclass(user_class_type_1, RestApiUser | GrizzlyUser)
    user_class_type_1 = cast('type[RestApiUser]', user_class_type_1)
    assert user_class_type_1.__name__ == f'grizzly.users.RestApiUser_{scenario.identifier}'
    assert user_class_type_1.weight == 1
    assert user_class_type_1.fixed_count == 0
    assert user_class_type_1.sticky_tag is None
    assert user_class_type_1.__scenario__ is scenario
    assert user_class_type_1.host == 'http://localhost:8000'
    assert user_class_type_1.__module__ == 'grizzly.users.restapi'
    assert user_class_type_1.__context__ == {
        'log_all_requests': False,
        'verify_certificates': True,
        'timeout': 60,
        'auth': {
            'refresh_time': 3000,
            'provider': None,
            'tenant': None,
            'client': {
                'id': None,
                'secret': None,
                'resource': None,
                'cert_file': None,
                'key_file': None,
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
        'verify_certificates': True,
        'timeout': 60,
        'auth': {
            'refresh_time': 3000,
            'provider': None,
            'tenant': None,
            'client': {
                'id': None,
                'secret': None,
                'resource': None,
                'cert_file': None,
                'key_file': None,
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

    scenario = GrizzlyContextScenario(1, behave=behave_fixture.create_scenario('TestTestTest'), grizzly=behave_fixture.grizzly)
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

    assert issubclass(user_class_type_2, RestApiUser | GrizzlyUser)
    user_class_type_2 = cast('type[RestApiUser]', user_class_type_2)
    assert user_class_type_2.__name__ == f'RestApiUser_{scenario.identifier}'
    assert user_class_type_2.weight == 1
    assert user_class_type_2.fixed_count == 100
    assert user_class_type_2.sticky_tag == 'foobar'
    assert user_class_type_2.__scenario__ is scenario
    assert user_class_type_2.host == 'http://localhost:8001'
    assert user_class_type_2.__module__ == 'grizzly.users.restapi'
    assert user_class_type_2.__context__ == {
        'log_all_requests': True,
        'test': {
            'value': 1,
        },
        'verify_certificates': True,
        'timeout': 60,
        'auth': {
            'refresh_time': 1337,
            'provider': 'https://auth.example.com',
            'tenant': None,
            'client': {
                'id': None,
                'secret': None,
                'resource': None,
                'cert_file': None,
                'key_file': None,
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
        'test': {
            'value': 1,
        },
        'verify_certificates': True,
        'timeout': 60,
        'auth': {
            'refresh_time': 1337,
            'provider': 'https://auth.example.com',
            'tenant': None,
            'client': {
                'id': None,
                'secret': None,
                'resource': None,
                'cert_file': None,
                'key_file': None,
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

    scenario = GrizzlyContextScenario(1, behave=behave_fixture.create_scenario('TestTestTest2'), grizzly=behave_fixture.grizzly)
    scenario.user.class_name = 'RestApiUser'
    scenario.context = {'test': {'value': 'hello world', 'description': 'simple text'}}
    user_class_type_3 = create_user_class_type(scenario, {'test': {'value': 1}})
    user_class_type_3.host = 'http://localhost:8002'

    assert issubclass(user_class_type_3, RestApiUser | GrizzlyUser)
    assert user_class_type_3.__name__ == f'RestApiUser_{scenario.identifier}'
    assert user_class_type_3.weight == 1
    assert user_class_type_3.fixed_count == 0
    assert user_class_type_3.__scenario__ is scenario
    assert user_class_type_3.host == 'http://localhost:8002'
    assert user_class_type_3.__module__ == 'grizzly.users.restapi'
    assert user_class_type_3.__context__ == {
        'log_all_requests': False,
        'test': {
            'value': 'hello world',
            'description': 'simple text',
        },
        'verify_certificates': True,
        'timeout': 60,
        'auth': {
            'refresh_time': 3000,
            'provider': None,
            'tenant': None,
            'client': {
                'id': None,
                'secret': None,
                'resource': None,
                'cert_file': None,
                'key_file': None,
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

    assert issubclass(user_class_type_3, RestApiUser | GrizzlyUser)
    assert user_class_type_3.__name__ == f'RestApiUser_{scenario.identifier}'
    assert user_class_type_3.weight == 1
    assert user_class_type_3.fixed_count == 10
    assert user_class_type_3.__scenario__ is scenario
    assert user_class_type_3.host == 'http://localhost:8002'
    assert user_class_type_3.__module__ == 'grizzly.users.restapi'
    assert user_class_type_3.__context__ == {
        'log_all_requests': False,
        'test': {
            'value': 'hello world',
            'description': 'simple text',
        },
        'verify_certificates': True,
        'timeout': 60,
        'auth': {
            'refresh_time': 3000,
            'provider': None,
            'tenant': None,
            'client': {
                'id': None,
                'secret': None,
                'resource': None,
                'cert_file': None,
                'key_file': None,
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

    scenario = GrizzlyContextScenario(1, behave=behave_fixture.create_scenario('A scenario description'), grizzly=behave_fixture.grizzly)
    scenario.user.class_name = 'DoNotExistInGrizzlyUsersUser'

    with pytest.raises(AttributeError, match=r"module 'grizzly\.users' has no attribute 'DoNotExistInGrizzlyUsersUser'"):
        create_user_class_type(scenario)


def test_create_scenario_class_type(behave_fixture: BehaveFixture) -> None:
    scenario = GrizzlyContextScenario(1, behave=behave_fixture.create_scenario('A scenario description'), grizzly=behave_fixture.grizzly)

    with pytest.raises(ModuleNotFoundError, match="No module named 'custom'"):
        create_scenario_class_type('custom.tasks.CustomTasks', scenario)

    task_class_type_1 = create_scenario_class_type('grizzly.scenarios.IteratorScenario', scenario)

    assert issubclass(task_class_type_1, IteratorScenario | TaskSet)
    assert task_class_type_1.__name__ == 'IteratorScenario_001'
    assert task_class_type_1.__module__ == 'grizzly.scenarios.iterator'
    assert getattr(task_class_type_1, 'pace_time', '') is None
    last_task_1 = task_class_type_1.tasks[-1]
    assert isinstance(last_task_1, FunctionType)
    task_class_type_1.populate(RequestTask(RequestMethod.POST, name='test-request-1', endpoint='/api/test'))
    assert task_class_type_1.tasks[-1] is last_task_1
    assert last_task_1.__name__ == 'pace'

    scenario = GrizzlyContextScenario(2, behave=behave_fixture.create_scenario('TestTestTest'), grizzly=behave_fixture.grizzly)
    scenario.pace = '2000'
    task_class_type_2 = create_scenario_class_type('IteratorScenario', scenario)
    assert issubclass(task_class_type_2, IteratorScenario | TaskSet)
    assert task_class_type_2.__name__ == 'IteratorScenario_002'
    assert task_class_type_2.__module__ == 'grizzly.scenarios.iterator'
    assert getattr(task_class_type_2, 'pace_time', '') == '2000'
    last_task_2 = task_class_type_2.tasks[-1]
    assert isinstance(last_task_2, FunctionType)
    task_class_type_2.populate(RequestTask(RequestMethod.POST, name='test-request-2', endpoint='/api/test'))
    assert task_class_type_2.tasks[-1] is last_task_2
    assert last_task_2.__name__ == 'pace'

    assert task_class_type_1.tasks != task_class_type_2.tasks

    scenario = GrizzlyContextScenario(3, behave=behave_fixture.create_scenario('A scenario description'), grizzly=behave_fixture.grizzly)
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

    assert in_correct_section(cast('FunctionType', step_custom), ['grizzly.steps.scenario'])


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
    assert flatten(
        {
            'hello': {'world': {'foo': {'bar': 'foobar'}}},
            'foo': {'bar': 'hello world!'},
        },
    ) == {
        'hello.world.foo.bar': 'foobar',
        'foo.bar': 'hello world!',
    }


def test_unflatten() -> None:
    assert unflatten('hello.world.foo.bar', 'foobar') == {
        'hello': {'world': {'foo': {'bar': 'foobar'}}},
    }

    assert unflatten('foo.bar', 'hello world!') == {'foo': {'bar': 'hello world!'}}
