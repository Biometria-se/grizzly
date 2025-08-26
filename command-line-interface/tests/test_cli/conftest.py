"""Configuration of pytest."""

from __future__ import annotations

import sys
from os import environ
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from test_cli.fixtures import (
    End2EndFixture,
)

if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Generator

    from _pytest.config import Config
    from _pytest.fixtures import SubRequest
    from _pytest.tmpdir import TempPathFactory

E2E_RUN_MODE = environ.get('E2E_RUN_MODE', 'local')
E2E_RUN_DIST = environ.get('E2E_RUN_DIST', 'False').lower() == 'True'.lower()


PYTEST_TIMEOUT = 300 if E2E_RUN_DIST or E2E_RUN_MODE == 'dist' else 150

# programatically load plugin, since pytest is started with `-p no:requests_mock` in pyproject.toml[tool.pytest.ini_options::addopts]
pytest_plugins = ['requests_mock.contrib._pytest_plugin']

if sys.platform == 'darwin' and PYTEST_TIMEOUT > 150:
    PYTEST_TIMEOUT = 500


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


def _e2e_fixture(tmp_path_factory: TempPathFactory, request: SubRequest) -> Generator[End2EndFixture, None, None]:
    distributed = request.param if hasattr(request, 'param') else E2E_RUN_MODE == 'dist'

    with End2EndFixture(tmp_path_factory, distributed=distributed) as fixture:
        yield fixture


e2e_fixture = pytest.fixture(scope='session', params=[False, True] if E2E_RUN_DIST else None)(_e2e_fixture)
