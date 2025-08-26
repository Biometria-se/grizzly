"""Configuration of pytest."""

from __future__ import annotations

from gevent import monkey

monkey.patch_all()

from os import environ
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from .fixtures import (
    AtomicVariableCleanupFixture,
    BehaveFixture,
    CwdFixture,
    End2EndFixture,
    EnvFixture,
    GrizzlyFixture,
    LocustFixture,
    NoopZmqFixture,
    RequestTaskFixture,
    ResponseContextManagerFixture,
)
from .webserver import Webserver

if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Generator

    from _pytest.config import Config
    from _pytest.fixtures import SubRequest
    from _pytest.tmpdir import TempPathFactory
    from pytest_mock.plugin import MockerFixture

E2E_RUN_MODE = environ.get('E2E_RUN_MODE', 'local')
E2E_RUN_DIST = environ.get('E2E_RUN_DIST', 'False').lower() == 'True'.lower()


PYTEST_TIMEOUT = 600 if E2E_RUN_DIST or E2E_RUN_MODE == 'dist' else 180


@pytest.fixture(autouse=True)
def run_before_and_after_tests(tmp_path_factory: TempPathFactory) -> Generator[None, None, None]:
    original_tmp_path = tmp_path_factory._basetemp
    test_root = (Path(__file__).parent / '..' / '..' / '.pytest_tmp').resolve()
    tmp_path_factory._basetemp = test_root
    tmp_path_factory._basetemp.mkdir(exist_ok=True)

    try:
        yield
    finally:
        tmp_path_factory._basetemp = original_tmp_path


# if we're only running E2E tests, set global timeout
def pytest_configure(config: Config) -> None:
    target = getattr(config.known_args_namespace, 'file_or_dir', ['foobar'])
    if len(target) > 0 and 'e2e' in target[0]:
        config._inicache['timeout'] = PYTEST_TIMEOUT


# also, add markers for each test function that starts with test_e2e_, if we're running everything
def pytest_collection_modifyitems(items: list[pytest.Function]) -> None:
    for item in items:
        if item.originalname.startswith('test_e2e_') and item.get_closest_marker('timeout') is None:
            item.add_marker(pytest.mark.timeout(PYTEST_TIMEOUT))


def _atomicvariable_cleanup() -> Generator[AtomicVariableCleanupFixture, None, None]:
    yield AtomicVariableCleanupFixture()


def _locust_fixture(tmp_path_factory: TempPathFactory) -> Generator[LocustFixture, None, None]:
    with LocustFixture(tmp_path_factory) as fixture:
        yield fixture


def _behave_fixture(locust_fixture: LocustFixture) -> Generator[BehaveFixture, None, None]:
    with BehaveFixture(locust_fixture) as fixture:
        yield fixture


def _request_task(tmp_path_factory: TempPathFactory, behave_fixture: BehaveFixture) -> Generator[RequestTaskFixture, None, None]:
    with RequestTaskFixture(tmp_path_factory, behave_fixture) as fixture:
        yield fixture


def _grizzly_fixture(request_task: RequestTaskFixture, behave_fixture: BehaveFixture) -> Generator[GrizzlyFixture, None, None]:
    with GrizzlyFixture(request_task, behave_fixture) as fixture:
        yield fixture


def _noop_zmq(mocker: MockerFixture) -> Generator[NoopZmqFixture, None, None]:
    yield NoopZmqFixture(mocker)


def _response_context_manager() -> Generator[ResponseContextManagerFixture, None, None]:
    yield ResponseContextManagerFixture()


def _webserver() -> Generator[Webserver, None, None]:
    with Webserver() as fixture:
        yield fixture


def _e2e_fixture(tmp_path_factory: TempPathFactory, webserver: Webserver, request: SubRequest) -> Generator[End2EndFixture, None, None]:
    distributed = request.param if hasattr(request, 'param') else E2E_RUN_MODE == 'dist'

    with End2EndFixture(tmp_path_factory, webserver, distributed=distributed) as fixture:
        yield fixture


def _env_fixture() -> Generator[EnvFixture, None, None]:
    yield EnvFixture()


def _cwd_fixture() -> Generator[CwdFixture, None, None]:
    yield CwdFixture()


cleanup = pytest.fixture()(_atomicvariable_cleanup)
locust_fixture = pytest.fixture(scope='function')(_locust_fixture)
behave_fixture = pytest.fixture(scope='function')(_behave_fixture)
request_task = pytest.fixture(scope='function')(_request_task)
grizzly_fixture = pytest.fixture(scope='function')(_grizzly_fixture)
noop_zmq = pytest.fixture()(_noop_zmq)
response_context_manager_fixture = pytest.fixture()(_response_context_manager)
webserver = pytest.fixture(scope='session')(_webserver)
e2e_fixture = pytest.fixture(scope='session', params=[False, True] if E2E_RUN_DIST else None)(_e2e_fixture)
env_fixture = pytest.fixture(scope='function')(_env_fixture)
cwd_fixture = pytest.fixture()(_cwd_fixture)
