from typing import Generator

import pytest

from _pytest.tmpdir import TempPathFactory

from pytest_mock.plugin import MockerFixture

from .fixtures import (
    AtomicVariableCleanupFixture,
    LocustFixture,
    NoopZmqFixture,
    ParamikoFixture,
    BehaveFixture,
    RequestTaskFixture,
    GrizzlyFixture,
    ResponseContextManagerFixture,
    Webserver,
)


def _atomicvariable_cleanup() -> Generator[AtomicVariableCleanupFixture, None, None]:
    yield AtomicVariableCleanupFixture()


@pytest.mark.usefixtures('tmp_path_factory')
def _locust_fixture(tmp_path_factory: TempPathFactory) -> Generator[LocustFixture, None, None]:
    with LocustFixture(tmp_path_factory) as fixture:
        yield fixture


@pytest.mark.usefixtures('mocker')
def _paramiko_fixture(mocker: MockerFixture) -> Generator[ParamikoFixture, None, None]:
    yield ParamikoFixture(mocker)


@pytest.mark.usefixtures('locust_fixture')
def _behave_fixture(locust_fixture: LocustFixture) -> Generator[BehaveFixture, None, None]:
    with BehaveFixture(locust_fixture) as fixture:
        yield fixture


@pytest.mark.usefixtures('tmp_path_factory')
def _request_task(tmp_path_factory: TempPathFactory) -> Generator[RequestTaskFixture, None, None]:
    with RequestTaskFixture(tmp_path_factory) as fixture:
        yield fixture


@pytest.mark.usefixtures('request_task', 'behave_fixture')
def _grizzly_fixture(request_task: RequestTaskFixture, behave_fixture: BehaveFixture) -> Generator[GrizzlyFixture, None, None]:
    with GrizzlyFixture(request_task, behave_fixture) as fixture:
        yield fixture


@pytest.mark.usefixtures('mocker')
def _noop_zmq(mocker: MockerFixture) -> Generator[NoopZmqFixture, None, None]:
    yield NoopZmqFixture(mocker)


def _response_context_manager() -> Generator[ResponseContextManagerFixture, None, None]:
    yield ResponseContextManagerFixture()


def _webserver() -> Generator[Webserver, None, None]:
    with Webserver() as fixture:
        yield fixture


cleanup = pytest.fixture()(_atomicvariable_cleanup)
locust_fixture = pytest.fixture()(_locust_fixture)
paramiko_fixture = pytest.fixture()(_paramiko_fixture)
behave_fixture = pytest.fixture()(_behave_fixture)
request_task = pytest.fixture()(_request_task)
grizzly_fixture = pytest.fixture(scope='function')(_grizzly_fixture)
noop_zmq = pytest.fixture()(_noop_zmq)
response_context_manager_fixture = pytest.fixture()(_response_context_manager)
webserver = pytest.fixture(scope='session')(_webserver)
