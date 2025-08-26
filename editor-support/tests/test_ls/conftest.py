from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from test_ls.fixtures import (
    CwdFixture,
    LspFixture,
    ServerConfigurationIndexUrlFixture,
    ServerInitializeFixture,
    ServerInstallFixture,
    ServerPipInstallUpgradeFixture,
    ServerTextDocumentCompletionFixture,
    ServerUseVirtualEnvironmentFixture,
)

if TYPE_CHECKING:
    from collections.abc import Generator

    from _pytest.tmpdir import TempPathFactory
    from pytest_mock.plugin import MockerFixture


def _lsp_fixture() -> Generator[LspFixture, None, None]:
    with LspFixture() as fixture:
        yield fixture


lsp_fixture = pytest.fixture(scope='session')(_lsp_fixture)

GRIZZLY_PROJECT = (Path(__file__) / '..' / '..' / '..' / 'tests' / 'project').resolve()

assert GRIZZLY_PROJECT.is_dir()


# give E2E tests a little bit more time
def pytest_collection_modifyitems(items: list[pytest.Function]) -> None:
    for item in items:
        if 'e2e' in item.path.as_posix() and item.get_closest_marker('timeout') is None:
            item.add_marker(pytest.mark.timeout(300))


@pytest.fixture
def cwd_fixture() -> Generator[CwdFixture, None, None]:
    fixture = CwdFixture()
    try:
        yield fixture
    finally:
        pass


@pytest.fixture
def server_pip_install_upgrade_fixture(lsp_fixture: LspFixture, mocker: MockerFixture, tmp_path_factory: TempPathFactory) -> Generator[ServerPipInstallUpgradeFixture, None, None]:
    fixture = ServerPipInstallUpgradeFixture(lsp_fixture, mocker, tmp_path_factory)
    try:
        yield fixture
    finally:
        fixture.done()


@pytest.fixture
def server_install_fixture(lsp_fixture: LspFixture, mocker: MockerFixture, tmp_path_factory: TempPathFactory) -> Generator[ServerInstallFixture, None, None]:
    fixture = ServerInstallFixture(lsp_fixture, mocker, tmp_path_factory)
    try:
        yield fixture
    finally:
        fixture.done()


@pytest.fixture
def server_use_virtual_environment_fixture(
    lsp_fixture: LspFixture, mocker: MockerFixture, tmp_path_factory: TempPathFactory
) -> Generator[ServerUseVirtualEnvironmentFixture, None, None]:
    fixture = ServerUseVirtualEnvironmentFixture(lsp_fixture, mocker, tmp_path_factory)
    try:
        yield fixture
    finally:
        fixture.done()


@pytest.fixture
def server_configuration_index_url_fixture(lsp_fixture: LspFixture, mocker: MockerFixture) -> Generator[ServerConfigurationIndexUrlFixture, None, None]:
    fixture = ServerConfigurationIndexUrlFixture(lsp_fixture, mocker)
    try:
        yield fixture
    finally:
        fixture.done()
        fixture.ls.client_settings.clear()
        fixture.ls.index_url = None


@pytest.fixture
def server_initialize_fixture(lsp_fixture: LspFixture, mocker: MockerFixture) -> Generator[ServerInitializeFixture, None, None]:
    fixture = ServerInitializeFixture(lsp_fixture, mocker)
    try:
        yield fixture
    finally:
        fixture.done()


@pytest.fixture
def server_text_document_completion_fixture(lsp_fixture: LspFixture, mocker: MockerFixture) -> Generator[ServerTextDocumentCompletionFixture, None, None]:
    fixture = ServerTextDocumentCompletionFixture(lsp_fixture, mocker)
    try:
        yield fixture
    finally:
        fixture.done()
