from typing import Callable

import pytest

from pytest_mock import mocker, MockerFixture

from grizzly.types import RequestMethod
from grizzly.task import UntilRequestTask, RequestTask
from grizzly_extras.transformer import TransformerContentType
from ..fixtures import grizzly_context, request_task, behave_context, locust_environment  # pylint: disable=unused-import

class TestUntilRequestTask:
    @pytest.mark.usefixtures('grizzly_context')
    def test(self, grizzly_context: Callable, mocker: MockerFixture) -> None:
        request = RequestTask(RequestMethod.GET, name='test', endpoint='/api/test')

        with pytest.raises(ValueError) as ve:
            task = UntilRequestTask(request, '$.`this`[?status="ready"]')
        assert 'content type must be specified for request' in str(ve)

        request.response.content_type = TransformerContentType.JSON
        task = UntilRequestTask(request, '$.`this`[?status="ready"]')

        assert task.condition == '$.`this`[?status="ready"]'
        assert task.wait == 1.0
        assert task.retries == 3

        task = UntilRequestTask(request, '$.`this`[?status="ready"] | wait=100, retries=10')

        assert task.condition == '$.`this`[?status="ready"]'
        assert task.wait == 100
        assert task.retries == 10

    @pytest.mark.usefixtures('grizzly_context')
    def test_data_table(self, grizzly_context: Callable, mocker: MockerFixture) -> None:
        pass
