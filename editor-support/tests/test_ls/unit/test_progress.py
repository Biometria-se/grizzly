from __future__ import annotations

from typing import TYPE_CHECKING
from unittest.mock import call

from grizzly_ls.server.progress import Progress
from lsprotocol import types as lsp

from test_ls.helpers import ANY, SOME

if TYPE_CHECKING:
    from pytest_mock import MockerFixture

    from test_ls.fixtures import LspFixture


def test_progress(lsp_fixture: LspFixture, mocker: MockerFixture) -> None:
    server = lsp_fixture.server

    progress = Progress(server, title='test')

    assert progress.progress is server.progress
    assert progress.title == 'test'
    assert progress.logger is server.logger
    assert isinstance(progress.token, str)

    report_spy = mocker.spy(progress, 'report')
    progress_create_mock = mocker.patch.object(progress.progress, 'create', return_value=None)
    progress_begin_mock = mocker.patch.object(progress.progress, 'begin', return_value=None)
    progress_end_mock = mocker.patch.object(
        progress.progress,
        'end',
        return_value=None,
    )
    progress_report_mock = mocker.patch.object(progress.progress, 'report', return_value=None)

    with progress as p:
        p.report('first', 50)
        p.report('second', 99)

    progress_create_mock.assert_called_once_with(progress.token, progress.callback)
    progress_begin_mock.assert_called_once_with(progress.token, SOME(lsp.WorkDoneProgressBegin, title=progress.title, percentage=0, cancellable=False))
    progress_end_mock.assert_called_once_with(progress.token, ANY(lsp.WorkDoneProgressEnd))

    assert progress_report_mock.mock_calls == [
        call(progress.token, SOME(lsp.WorkDoneProgressReport, message='first', percentage=50)),
        call(progress.token, SOME(lsp.WorkDoneProgressReport, message='second', percentage=99)),
        call(progress.token, SOME(lsp.WorkDoneProgressReport, message=None, percentage=100)),
    ]
    assert report_spy.mock_calls == [
        call('first', 50),
        call('second', 99),
        call(None, 100),
    ]
