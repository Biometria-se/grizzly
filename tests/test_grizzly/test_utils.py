import os

from typing import Any, Callable, Optional, Type, Dict, cast
from types import FunctionType

import pytest

from locust.user.users import User
from locust.env import Environment
from locust import TaskSet
from behave.runner import Context
from behave.model import Scenario
from behave.model_core import Status

from grizzly.utils import ModuleLoader
from grizzly.utils import (
    catch,
    create_task_class_type,
    create_user_class_type,
    fail_direct,
    create_context_variable,
    resolve_variable,
    _objectify,
    transform,
    in_correct_section
)
from grizzly.types import RequestMethod
from grizzly.context import GrizzlyContext, GrizzlyContextScenario
from grizzly.task import RequestTask
from grizzly.users import RestApiUser
from grizzly.tasks import IteratorTasks

# pylint: disable=unused-import
from .fixtures import (
    grizzly_context,
    locust_environment,
    request_task,
    behave_context,
    behave_runner,
    behave_scenario,
)

from .testdata.fixtures import cleanup


class TestModuleLoader:
    def test_load_class_non_existent(self) -> None:
        class_name = 'ANonExistingModule'

        with pytest.raises(ModuleNotFoundError):
            ModuleLoader[User].load('a.non.existing.package', class_name)

        with pytest.raises(AttributeError):
            ModuleLoader[User].load('grizzly.users', class_name)


    @pytest.mark.usefixtures('locust_environment')
    def test_load_user_class(self, locust_environment: Environment) -> None:
        try:
            test_context = GrizzlyContext()
            test_context.scenario.context['host'] = 'test'
            user_class_name = 'RestApiUser'
            for user_package in ['', 'grizzly.users.', 'grizzly.users.restapi.']:
                user_class_name_value = f'{user_package}{user_class_name}'
                user_class = cast(Type[User], ModuleLoader[User].load('grizzly.users', user_class_name_value))
                user_class.host = test_context.scenario.context['host']
                assert user_class.__module__ == 'grizzly.users.restapi'
                assert user_class.host == 'test'
                assert hasattr(user_class, 'tasks')

                # try to initialize it, without any token information
                user_class_instance = user_class(locust_environment)

                # with token context
                test_context.scenario.context['token'] = {
                    'client_secret': 'asdf',
                    'client_id': 'asdf',
                    'url': 'http://test',
                    'resource': None,
                }
                user_class_instance = user_class(locust_environment)

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


@pytest.mark.usefixtures('behave_context', 'behave_scenario')
def test_catch(behave_context: Context, behave_scenario: Scenario) -> None:
    @catch(KeyboardInterrupt)
    def raises_KeyboardInterrupt(context: Context, scenario: Scenario) -> None:
        raise KeyboardInterrupt()

    try:
        raises_KeyboardInterrupt(behave_context, behave_scenario)
    except KeyboardInterrupt:
        pytest.fail(f'function raised KeyboardInterrupt, when it should not have')

    assert behave_context.failed
    assert behave_scenario.status == Status.failed
    behave_context._set_root_attribute('failed', False)
    behave_scenario.set_status(Status.undefined)

    @catch(ValueError)
    def raises_ValueError_not(context: Context, scenario: Scenario) -> None:
        raise KeyboardInterrupt()

    with pytest.raises(KeyboardInterrupt):
        raises_ValueError_not(behave_context, behave_scenario)

    assert not behave_context.failed
    assert not behave_scenario.status == Status.failed
    behave_context._set_root_attribute('failed', False)
    behave_scenario.set_status(Status.undefined)

    @catch(ValueError)
    def raises_ValueError(context: Context, scenario: Optional[Scenario] = None) -> None:
        raise ValueError()

    with pytest.raises(ValueError):
        raises_ValueError(behave_context)

    @catch(NotImplementedError)
    def no_scenario_argument(context: Context, other: str) -> None:
        raise NotImplementedError()

    with pytest.raises(NotImplementedError):
        no_scenario_argument(behave_context, 'not a scenario')

    try:
        raises_ValueError(behave_context, behave_scenario)
    except ValueError:
        pytest.fail(f'function raised ValueError, when it should not have')


@pytest.mark.usefixtures('behave_context')
def test_fail_directly(behave_context: Context) -> None:
    behave_context.config.stop = False
    behave_context.config.verbose = True

    with fail_direct(behave_context):
        assert behave_context.config.stop == True
        assert behave_context.config.verbose == False

    assert behave_context.config.stop == False
    assert behave_context.config.verbose == True


@pytest.mark.usefixtures('locust_environment')
def test_create_user_class_type(locust_environment: Environment) -> None:
    scenario = GrizzlyContextScenario()
    scenario.name = 'A scenario description'

    with pytest.raises(ValueError):
        create_user_class_type(scenario)

    scenario.user_class_name = 'RestApiUser'
    user_class_type_1 = create_user_class_type(scenario)
    user_class_type_1.host = 'http://localhost:8000'

    assert issubclass(user_class_type_1, (RestApiUser, User))
    assert user_class_type_1.__name__ == f'RestApiUser_{scenario.identifier}'
    assert user_class_type_1.host == 'http://localhost:8000'
    assert user_class_type_1.__module__ == 'locust.user.users'
    assert user_class_type_1._context == {
        'verify_certificates': True,
        'auth': {
            'refresh_time': 3000,
            'url': None,
            'client': {
                'id': None,
                'secret': None,
                'resource': None,
                'tenant': None,
            },
            'user': {
                'username': None,
                'password': None,
                'redirect_uri': None,
            }
        }
    }
    user_type_1 = user_class_type_1(locust_environment)

    assert user_type_1.context() == {
        'log_all_requests': False,
        'variables': {},
        'verify_certificates': True,
        'auth': {
            'refresh_time': 3000,
            'url': None,
            'client': {
                'id': None,
                'secret': None,
                'resource': None,
                'tenant': None,
            },
            'user': {
                'username': None,
                'password': None,
                'redirect_uri': None,
            }
        }
    }

    scenario = GrizzlyContextScenario()
    scenario.name = 'TestTestTest'
    scenario.user_class_name = 'RestApiUser'
    user_class_type_2 = create_user_class_type(
        scenario,
        {
            'test': {
                'value': 1,
            },
            'log_all_requests': True,
            'auth': {
                'refresh_time': 1337,
                'url': 'https://auth.example.com',
                'user': {
                    'username': 'grizzly-user',
                }
            },
        }
    )
    user_class_type_2.host = 'http://localhost:8001'

    assert issubclass(user_class_type_2, (RestApiUser, User))
    assert user_class_type_2.__name__ == f'RestApiUser_{scenario.identifier}'
    assert user_class_type_2.host == 'http://localhost:8001'
    assert user_class_type_2.__module__ == 'locust.user.users'
    assert user_class_type_2._context == {
        'log_all_requests': True,
        'test': {
            'value': 1,
        },
        'verify_certificates': True,
        'auth': {
            'refresh_time': 1337,
            'url': 'https://auth.example.com',
            'client': {
                'id': None,
                'secret': None,
                'resource': None,
                'tenant': None,
            },
            'user': {
                'username': 'grizzly-user',
                'password': None,
                'redirect_uri': None,
            }
        }
    }

    user_type_2 = user_class_type_2(locust_environment)
    assert user_type_2.context() == {
        'log_all_requests': True,
        'variables': {},
        'test': {
            'value': 1,
        },
        'verify_certificates': True,
        'auth': {
            'refresh_time': 1337,
            'url': 'https://auth.example.com',
            'client': {
                'id': None,
                'secret': None,
                'resource': None,
                'tenant': None,
            },
            'user': {
                'username': 'grizzly-user',
                'password': None,
                'redirect_uri': None,
            }
        }
    }

    scenario = GrizzlyContextScenario()
    scenario.name = 'TestTestTest2'
    scenario.user_class_name = 'RestApiUser'
    scenario.context = {'test': {'value': 'hello world', 'description': 'simple text'}}
    user_class_type_3 = create_user_class_type(scenario, {'test': {'value': 1}})
    user_class_type_3.host = 'http://localhost:8002'

    assert issubclass(user_class_type_3, (RestApiUser, User))
    assert user_class_type_3.__name__ == f'RestApiUser_{scenario.identifier}'
    assert user_class_type_3.host == 'http://localhost:8002'
    assert user_class_type_3.__module__ == 'locust.user.users'
    assert user_class_type_3._context == {
        'test': {
            'value': 'hello world',
            'description': 'simple text',
        },
        'verify_certificates': True,
        'auth': {
            'refresh_time': 3000,
            'url': None,
            'client': {
                'id': None,
                'secret': None,
                'resource': None,
                'tenant': None,
            },
            'user': {
                'username': None,
                'password': None,
                'redirect_uri': None,
            }
        }
    }

    assert user_class_type_1.host is not user_class_type_2.host
    assert user_class_type_2.host is not user_class_type_3.host

    with pytest.raises(AttributeError):
        scenario = GrizzlyContextScenario()
        scenario.name = 'A scenario description'
        scenario.user_class_name = 'DoNotExistInGrizzlyUsersUser'
        create_user_class_type(scenario)


def test_create_task_class_type() -> None:
    scenario = GrizzlyContextScenario()
    scenario.name = 'A scenario description'
    task_class_type_1 = create_task_class_type('IteratorTasks', scenario)

    assert issubclass(task_class_type_1, (IteratorTasks, TaskSet))
    assert task_class_type_1.__name__ == 'IteratorTasks_25867809'
    assert task_class_type_1.__module__ == 'locust.user.sequential_taskset'
    task_class_type_1.add_scenario_task(RequestTask(RequestMethod.POST, name='test-request', endpoint='/api/test'))

    scenario = GrizzlyContextScenario()
    scenario.name = 'TestTestTest'
    task_class_type_2 = create_task_class_type('IteratorTasks', scenario)
    assert issubclass(task_class_type_2, (IteratorTasks, TaskSet))
    assert task_class_type_2.__name__ == 'IteratorTasks_cf4fa8aa'
    assert task_class_type_2.__module__ == 'locust.user.sequential_taskset'

    assert task_class_type_1.tasks != task_class_type_2.tasks

    with pytest.raises(AttributeError):
        scenario = GrizzlyContextScenario()
        scenario.name = 'A scenario description'
        create_task_class_type('DoesNotExistInGrizzlyScenariosModel', scenario)


def test_create_context_variable() -> None:
    grizzly = GrizzlyContext()

    try:
        assert create_context_variable(grizzly, 'test.value', '1') == {
            'test': {
                'value': 1,
            }
        }

        assert create_context_variable(grizzly, 'test.value', 'trUe') == {
            'test': {
                'value': True,
            }
        }

        assert create_context_variable(grizzly, 'test.value', 'AZURE') == {
            'test': {
                'value': 'AZURE',
            }
        }

        assert create_context_variable(grizzly, 'test.value', 'HOST') == {
            'test': {
                'value': 'HOST'
            }
        }

        with pytest.raises(AssertionError):
            create_context_variable(grizzly, 'test.value', '$env::HELLO_WORLD')

        os.environ['HELLO_WORLD'] = 'environment variable value'
        assert create_context_variable(grizzly, 'test.value', '$env::HELLO_WORLD') == {
            'test': {
                'value': 'environment variable value',
            }
        }

        os.environ['HELLO_WORLD'] = 'true'
        assert create_context_variable(grizzly, 'test.value', '$env::HELLO_WORLD') == {
            'test': {
                'value': True,
            }
        }

        os.environ['HELLO_WORLD'] = '1337'
        assert create_context_variable(grizzly, 'test.value', '$env::HELLO_WORLD') == {
            'test': {
                'value': 1337,
            }
        }

        with pytest.raises(AssertionError):
            create_context_variable(grizzly, 'test.value', '$conf::test.auth.user.username')

        grizzly.state.configuration['test.auth.user.username'] = 'username'
        assert create_context_variable(grizzly, 'test.value', '$conf::test.auth.user.username') == {
            'test': {
                'value': 'username',
            }
        }

        grizzly.state.configuration['test.auth.refresh_time'] = 3000
        assert create_context_variable(grizzly, 'test.value', '$conf::test.auth.refresh_time') == {
            'test': {
                'value': 3000,
            }
        }
    finally:
        GrizzlyContext.destroy()
        del os.environ['HELLO_WORLD']


def test_resolve_variable() -> None:
    grizzly = GrizzlyContext()

    try:
        assert 'test' not in grizzly.state.variables
        with pytest.raises(AssertionError):
            resolve_variable(grizzly, '{{ test }}')

        grizzly.state.variables['test'] = 'some value'
        assert resolve_variable(grizzly, '{{ test }}') == 'some value'

        assert resolve_variable(grizzly, "now | format='%Y-%m-%d %H'") == "now | format='%Y-%m-%d %H'"

        assert resolve_variable(grizzly, "{{ test }} | format='%Y-%m-%d %H'") == "some value | format='%Y-%m-%d %H'"

        assert resolve_variable(grizzly, 'static value') == 'static value'
        assert resolve_variable(grizzly, '"static value"') == 'static value'
        assert resolve_variable(grizzly, "'static value'") == 'static value'
        assert resolve_variable(grizzly, "'static' value") == "'static' value"
        assert resolve_variable(grizzly, "static 'value'") == "static 'value'"

        with pytest.raises(ValueError):
            resolve_variable(grizzly, "'static value\"")

        with pytest.raises(ValueError):
            resolve_variable(grizzly, "static 'value\"")

        with pytest.raises(ValueError):
            resolve_variable(grizzly, "'static\" value")

        grizzly.state.variables['number'] = 100
        assert resolve_variable(grizzly, '{{ (number * 0.25) | int }}') == 25

        assert resolve_variable(grizzly, '{{ (number * 0.25 * 0.2) | int }}') == 5

        try:
            with pytest.raises(AssertionError):
                resolve_variable(grizzly, '$env::HELLO_WORLD')

            os.environ['HELLO_WORLD'] = 'first environment variable!'

            assert resolve_variable(grizzly, '$env::HELLO_WORLD') == 'first environment variable!'

            os.environ['HELLO_WORLD'] = 'first "environment" variable!'
            assert resolve_variable(grizzly, '$env::HELLO_WORLD') == 'first "environment" variable!'
        finally:
            del os.environ['HELLO_WORLD']

        with pytest.raises(AssertionError):
            resolve_variable(grizzly, '$conf::sut.host')

        grizzly.state.configuration['sut.host'] = 'http://host.docker.internal:8003'

        assert resolve_variable(grizzly, '$conf::sut.host')

        grizzly.state.configuration['sut.greeting'] = 'hello "{{ test }}"!'
        assert resolve_variable(grizzly, '$conf::sut.greeting') == 'hello "{{ test }}"!'

        with pytest.raises(ValueError):
            resolve_variable(grizzly, '$test::hello')

        assert resolve_variable(grizzly, '') == ''
    finally:
        GrizzlyContext.destroy()


def test__objectify() -> None:
    testdata: Dict[str, Any] = {
        'AtomicInteger': {
            'test': 1337,
        },
        'test': 1338,
        'AtomicCsvRow': {
            'input': {
                'test1': 'hello',
                'test2': 'world!',
            }
        },
        'Test': {
            'test1': {
                'test2': {
                    'test3': 'value',
                }
            }
        }
    }

    obj = _objectify(testdata)

    assert (
        obj['AtomicInteger'].__module__ == 'grizzly.utils' and
        obj['AtomicInteger'].__class__.__name__ == 'Testdata'
    )
    assert getattr(obj['AtomicInteger'], 'test') == 1337
    assert isinstance(obj['test'], int)
    assert obj['test'] == 1338
    assert (
        obj['AtomicCsvRow'].__module__ == 'grizzly.utils' and
        obj['AtomicCsvRow'].__class__.__name__ == 'Testdata'
    )
    atomiccsvrow_input = getattr(obj['AtomicCsvRow'], 'input', None)
    assert atomiccsvrow_input is not None
    assert (
        atomiccsvrow_input.__module__ == 'grizzly.utils' and
        atomiccsvrow_input.__class__.__name__ == 'Testdata'
    )
    assert getattr(atomiccsvrow_input, 'test1', None) == 'hello'
    assert getattr(atomiccsvrow_input, 'test2', None) == 'world!'

    assert (
        obj['Test'].__module__ == 'grizzly.utils' and
        obj['Test'].__class__.__name__ == 'Testdata'
    )
    test = getattr(obj['Test'], 'test1', None)
    assert test is not None
    assert (
        test.__module__ == 'grizzly.utils' and
        test.__class__.__name__ == 'Testdata'
    )
    test = getattr(test, 'test2', None)
    assert test is not None
    assert (
        test.__module__ == 'grizzly.utils' and
        test.__class__.__name__ == 'Testdata'
    )
    test = getattr(test, 'test3', None)
    assert test is not None
    assert isinstance(test, str)
    assert test == 'value'


def test_transform_raw() -> None:
    data = {
        'test.number.value': 1337,
        'test.number.description': 'simple description',
        'test.string.value': 'hello world!',
        'test.bool.value': True,
    }

    actual = transform(data, True)

    assert actual == {
        'test': {
            'number': {
                'value': 1337,
                'description': 'simple description',
            },
            'string': {
                'value': 'hello world!',
            },
            'bool': {
                'value': True,
            }
        }
    }


@pytest.mark.usefixtures('cleanup')
def test_transform(cleanup: Callable) -> None:
    try:
        data: Dict[str, Any] = {
            'AtomicInteger.test': 1337,
            'test': 1338,
            'AtomicCsvRow.input.test1': 'hello',
            'AtomicCsvRow.input.test2': 'world!',
            'Test.test1.test2.test3': 'value',
            'Test.test1.test2.test4': 'value',
            'Test.test2.test3': 'value',
        }

        obj = transform(data)

        assert (
            obj['AtomicInteger'].__module__ == 'grizzly.utils' and
            obj['AtomicInteger'].__class__.__name__ == 'Testdata'
        )
        assert getattr(obj['AtomicInteger'], 'test', None) == 1337
        assert isinstance(obj['test'], int)
        assert obj['test'] == 1338
        assert (
            obj['AtomicCsvRow'].__module__ == 'grizzly.utils' and
            obj['AtomicCsvRow'].__class__.__name__ == 'Testdata'
        )
        assert (
            obj['AtomicCsvRow'].input.__module__ == 'grizzly.utils' and
            obj['AtomicCsvRow'].input.__class__.__name__ == 'Testdata'
        )
        assert getattr(obj['AtomicCsvRow'].input, 'test1', None) == 'hello'
        assert getattr(obj['AtomicCsvRow'].input, 'test2', None) == 'world!'
        assert (
            obj['Test'].__module__ == 'grizzly.utils' and
            obj['Test'].__class__.__name__ == 'Testdata'
        )
        test = getattr(obj['Test'], 'test1', None)
        assert test is not None
        assert (
            test.__module__ == 'grizzly.utils' and
            test.__class__.__name__ == 'Testdata'
        )
        test = getattr(test, 'test2', None)
        assert test is not None
        assert (
            test.__module__ == 'grizzly.utils' and
            test.__class__.__name__ == 'Testdata'
        )
        test = getattr(test, 'test3', None)
        assert test is not None
        assert isinstance(test, str)
        assert test == 'value'
    finally:
        cleanup()


def test_in_correct_section() -> None:
    from grizzly.steps import step_setup_iterations
    assert in_correct_section(step_setup_iterations, ['grizzly.steps.scenario'])
    assert not in_correct_section(step_setup_iterations, ['grizzly.steps.background'])

    def step_custom(context: Context) -> None:
        pass

    # force AttributeError, for when a step function isn't part of a module
    setattr(step_custom, '__module__', None)

    assert in_correct_section(cast(FunctionType, step_custom), ['grizzly.steps.scenario'])

