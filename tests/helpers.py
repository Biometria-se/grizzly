import inspect

from typing import Any, Dict, Optional, Tuple, List, Set, Type
from types import MethodType

from locust import task
from locust.user.task import TaskSet

from grizzly.users.meta import ContextVariables
from grizzly.types import RequestMethod
from grizzly.context import RequestContext


def clone_request(method: str, this: RequestContext) -> RequestContext:
    that = RequestContext(RequestMethod.from_string(method), name=this.name, endpoint=this.endpoint)
    that.source = this.source
    that.scenario = this.scenario
    that.template = this.template

    return that


class WaitCalled(Exception):
    time: float

    def __init__(self, time: float) -> None:
        super().__init__()

        self.time = time

class RequestCalled(Exception):
    endpoint: str
    request: RequestContext

    def __init__(self, request: RequestContext) -> None:
        super().__init__()

        self.endpoint = request.endpoint
        self.request = request


class TestUser(ContextVariables):
    __test__ = False

    _config_property: Optional[str] = None

    @property
    def config_property(self) -> Optional[str]:
        return self._config_property

    @config_property.setter
    def config_property(self, value: Optional[str]) -> None:
        self._config_property = value

    def request(self, request: RequestContext) -> None:
        raise RequestCalled(request)


class TestTaskSet(TaskSet):
    __test__ = False

    @task
    def task(self) -> None:
        self.user.request('payload.j2.json', {})

class ResultSuccess(Exception):
    pass


class ResultFailure(Exception):
    pass

def check_arguments(kwargs: Dict[str, Any]) -> Tuple[bool, List]:
    expected = ['request_type', 'name', 'response_time', 'response_length', 'context', 'exception']
    actual = list(kwargs.keys())
    expected.sort()
    actual.sort()

    diff = list(set(expected) - set(actual))

    return actual == expected, diff

class RequestEvent:
    def __init__(self, custom: bool = True):
        self.custom = custom

    def fire(self, *args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> None:
        if self.custom:
            valid, diff = check_arguments(kwargs)
            if not valid:
                raise AttributeError(f'missing required arguments: {diff}')

        if 'exception' in kwargs and kwargs['exception'] is not None:
            raise ResultFailure(kwargs['exception'])
        else:
            raise ResultSuccess()

class RequestSilentFailureEvent(RequestEvent):
    def fire(self, *args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> None:
        if self.custom:
            valid, diff = check_arguments(kwargs)
            if not valid:
                raise AttributeError(f'missing required arguments: {diff}')

        if 'exception' not in kwargs or kwargs['exception'] is None:
            raise ResultSuccess()


def get_property_decorated_attributes(target: Any) -> Set[str]:
    return set(
        [name
            for name, _ in inspect.getmembers(
                target,
                lambda p: isinstance(
                    p,
                    (property, MethodType)
                ) and not isinstance(
                    p,
                    (classmethod, MethodType)  # @classmethod anotated methods becomes @property
                )) if not name.startswith('_')
        ]
    )
