"""Unit tests of grizzly.steps.scenario.tasks.clients."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

import pytest
from grizzly.steps import (
    step_task_client_from_endpoint_payload,
    step_task_client_from_endpoint_payload_and_metadata,
    step_task_client_to_endpoint_file,
    step_task_client_to_endpoint_file_destination,
    step_task_client_to_endpoint_text,
)
from grizzly.tasks.clients import HttpClientTask
from grizzly.types import RequestMethod, pymqi

from test_framework.helpers import ANY

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.context import GrizzlyContext

    from test_framework.fixtures import BehaveFixture, GrizzlyFixture


def test_step_task_client_from_endpoint_payload_metadata(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast('GrizzlyContext', behave.grizzly)
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))
    behave.scenario = grizzly.scenario.behave

    step_task_client_from_endpoint_payload_and_metadata(behave, RequestMethod.GET, 'obscure.example.com', 'step-name', 'test', 'metadata')
    assert behave.exceptions == {behave.scenario.name: [ANY(AssertionError, message='could not find scheme in "obscure.example.com"')]}
    delattr(behave, 'exceptions')

    step_task_client_from_endpoint_payload_and_metadata(behave, RequestMethod.GET, 'obscure://obscure.example.com', 'step-name', 'test', 'metadata')
    assert behave.exceptions == {behave.scenario.name: [ANY(AssertionError, message='no client task registered for obscure')]}
    delattr(behave, 'exceptions')

    step_task_client_from_endpoint_payload_and_metadata(behave, RequestMethod.GET, 'http://www.example.org', 'step-name', 'test', 'metadata')
    assert behave.exceptions == {behave.scenario.name: [ANY(AssertionError, message='HttpClientTask: variable test has not been initialized')]}
    delattr(behave, 'exceptions')

    if pymqi.__name__ != 'grizzly_common.dummy_pymqi':
        step_task_client_from_endpoint_payload_and_metadata(behave, RequestMethod.GET, 'mq://mq.example.org', 'step-name', 'test', 'metadata')
        assert behave.exceptions == {behave.scenario.name: [ANY(AssertionError, message='MessageQueueClientTask: variable test has not been initialized')]}
        delattr(behave, 'exceptions')

    grizzly.scenario.variables['test'] = 'none'

    step_task_client_from_endpoint_payload_and_metadata(behave, RequestMethod.GET, 'http://www.example.org', 'step-name', 'test', 'metadata')
    assert behave.exceptions == {behave.scenario.name: [ANY(AssertionError, message='HttpClientTask: variable metadata has not been initialized')]}
    delattr(behave, 'exceptions')

    if pymqi.__name__ != 'grizzly_common.dummy_pymqi':
        step_task_client_from_endpoint_payload_and_metadata(behave, RequestMethod.GET, 'mq://mq.example.org', 'step-name', 'test', 'metadata')
        assert behave.exceptions == {behave.scenario.name: [ANY(AssertionError, message='MessageQueueClientTask: variable metadata has not been initialized')]}
        delattr(behave, 'exceptions')

    grizzly.scenario.variables['metadata'] = 'none'

    assert len(grizzly.scenario.tasks()) == 0
    step_task_client_from_endpoint_payload_and_metadata(behave, RequestMethod.GET, 'http://www.example.org', 'step-name', 'test', 'metadata')
    assert len(grizzly.scenario.tasks()) == 1

    grizzly.scenario.variables['endpoint_url'] = 'https://example.org'
    step_task_client_from_endpoint_payload_and_metadata(behave, RequestMethod.GET, 'https://{{ endpoint_url }}', 'step-name', 'test', 'metadata')

    task = grizzly.scenario.tasks()[-1]
    assert isinstance(task, HttpClientTask)
    assert task.endpoint == '{{ endpoint_url }}'
    assert sorted(task.get_templates()) == sorted(['{{ endpoint_url }}', '{{ test }} {{ metadata }}'])

    behave.text = '1=1'
    with pytest.raises(NotImplementedError, match='HttpClientTask has not implemented support for step text'):
        step_task_client_from_endpoint_payload_and_metadata(behave, RequestMethod.GET, 'https://{{ endpoint_url }}', 'step-name', 'test', 'metadata')
    assert behave.exceptions == {behave.scenario.name: [ANY(NotImplementedError, message='HttpClientTask has not implemented support for step text')]}
    delattr(behave, 'exceptions')

    print('=' * 200)

    behave.text = None
    step_task_client_from_endpoint_payload_and_metadata(behave, RequestMethod.POST, 'https://{{ endpoint_url }}', 'step-name', 'test', 'metadata')
    assert behave.exceptions == {
        behave.scenario.name: [
            ANY(AssertionError, message='chosen request method does not match direction "from"'),
        ],
    }
    delattr(behave, 'exceptions')


def test_step_task_client_from_endpoint_payload(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast('GrizzlyContext', behave.grizzly)
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))
    behave.scenario = grizzly.scenario.behave

    step_task_client_from_endpoint_payload(behave, RequestMethod.GET, 'obscure.example.com', 'step-name', 'test')
    assert behave.exceptions == {behave.scenario.name: [ANY(AssertionError, message='could not find scheme in "obscure.example.com"')]}
    delattr(behave, 'exceptions')

    step_task_client_from_endpoint_payload(behave, RequestMethod.GET, 'obscure://obscure.example.com', 'step-name', 'test')
    assert behave.exceptions == {behave.scenario.name: [ANY(AssertionError, message='no client task registered for obscure')]}
    delattr(behave, 'exceptions')

    step_task_client_from_endpoint_payload(behave, RequestMethod.GET, 'http://www.example.org', 'step-name', 'test')
    assert behave.exceptions == {behave.scenario.name: [ANY(AssertionError, message='HttpClientTask: variable test has not been initialized')]}
    delattr(behave, 'exceptions')

    if pymqi.__name__ != 'grizzly_common.dummy_pymqi':
        step_task_client_from_endpoint_payload(behave, RequestMethod.GET, 'mq://mq.example.org', 'step-name', 'test')
        assert behave.exceptions == {behave.scenario.name: [ANY(AssertionError, message='MessageQueueClientTask: variable test has not been initialized')]}
        delattr(behave, 'exceptions')

    grizzly.scenario.variables['test'] = 'none'

    assert len(grizzly.scenario.tasks()) == 0
    step_task_client_from_endpoint_payload(behave, RequestMethod.GET, 'http://www.example.org', 'step-name', 'test')
    assert len(grizzly.scenario.tasks()) == 1

    grizzly.scenario.variables['endpoint_url'] = 'https://example.org'
    step_task_client_from_endpoint_payload(behave, RequestMethod.GET, 'https://{{ endpoint_url }}', 'step-name', 'test')

    task = grizzly.scenario.tasks()[-1]
    assert isinstance(task, HttpClientTask)
    assert task.endpoint == '{{ endpoint_url }}'
    assert sorted(task.get_templates()) == sorted(['{{ endpoint_url }}', '{{ test }}'])

    behave.text = '1=1'
    with pytest.raises(NotImplementedError, match='HttpClientTask has not implemented support for step text'):
        step_task_client_from_endpoint_payload(behave, RequestMethod.GET, 'https://{{ endpoint_url }}', 'step-name', 'test')
    assert behave.exceptions == {behave.scenario.name: [ANY(NotImplementedError, message='HttpClientTask has not implemented support for step text')]}
    delattr(behave, 'exceptions')


@pytest.mark.parametrize('request_method', [RequestMethod.PUT, RequestMethod.POST])
def test_step_task_client_to_endpoint_file(grizzly_fixture: GrizzlyFixture, request_method: RequestMethod) -> None:
    behave = grizzly_fixture.behave.context
    grizzly = grizzly_fixture.grizzly
    grizzly.scenario.tasks.clear()

    behave.text = 'hello'

    assert len(grizzly.scenario.orphan_templates) == 0
    assert len(grizzly.scenario.tasks()) == 0

    step_task_client_to_endpoint_file(behave, request_method, 'file.json', 'http://example.org/put', 'step-name')
    assert behave.exceptions == {behave.scenario.name: [ANY(AssertionError, message='step text is not allowed for this step expression')]}
    delattr(behave, 'exceptions')

    behave.text = None

    step_task_client_to_endpoint_file(behave, request_method, 'file-{{ suffix }}.json', 'http://{{ url }}', 'step-name')
    assert behave.exceptions == {behave.scenario.name: [ANY(AssertionError, message='source file cannot be a template')]}
    delattr(behave, 'exceptions')

    step_task_client_to_endpoint_file(behave, request_method, 'file-test.json', 'http://{{ url }}', 'step-name')

    assert len(grizzly.scenario.tasks()) == 1
    task = grizzly.scenario.tasks()[-1]

    assert isinstance(task, HttpClientTask)
    assert task.source == 'file-test.json'
    assert task.destination is None
    assert task.endpoint == '{{ url }}'

    templates = task.get_templates()
    assert sorted(templates) == sorted(
        [
            '{{ url }}',
        ],
    )

    test_file = grizzly_fixture.test_context / 'requests' / 'file-test.json'
    test_file.parent.mkdir(exist_ok=True)
    test_file.write_text('foobar {{ foo }}!')

    try:
        step_task_client_to_endpoint_file(behave, request_method, 'file-test.json', 'http://{{ url }}', 'step-name-2')

        assert len(grizzly.scenario.tasks()) == 2
        task = grizzly.scenario.tasks()[-1]

        assert isinstance(task, HttpClientTask)
        assert task.source == 'foobar {{ foo }}!'
        assert task.destination is None
        assert task.endpoint == '{{ url }}'

        templates = task.get_templates()
        assert sorted(templates) == sorted(
            [
                '{{ url }}',
                'foobar {{ foo }}!',
            ],
        )
    finally:
        test_file.unlink()


@pytest.mark.parametrize('request_method', [RequestMethod.PUT, RequestMethod.POST])
def test_step_task_client_to_endpoint_file_destination(grizzly_fixture: GrizzlyFixture, request_method: RequestMethod) -> None:
    behave = grizzly_fixture.behave.context
    grizzly = grizzly_fixture.grizzly

    grizzly.scenario.tasks.clear()

    behave.text = 'hello'

    assert len(grizzly.scenario.orphan_templates) == 0
    assert len(grizzly.scenario.tasks()) == 0

    step_task_client_to_endpoint_file_destination(behave, request_method, 'file.json', 'http://example.org/put', 'step-name', 'uploaded-file.json')
    assert behave.exceptions == {behave.scenario.name: [ANY(AssertionError, message='step text is not allowed for this step expression')]}
    delattr(behave, 'exceptions')

    behave.text = None

    step_task_client_to_endpoint_file_destination(behave, request_method, 'file-{{ suffix }}.json', 'http://{{ url }}', 'step-name', 'uploaded-file-{{ suffix }}.json')
    assert behave.exceptions == {behave.scenario.name: [ANY(AssertionError, message='source file cannot be a template')]}
    delattr(behave, 'exceptions')

    step_task_client_to_endpoint_file_destination(behave, request_method, 'file-test.json', 'http://{{ url }}', 'step-name', 'uploaded-file-{{ suffix }}.json')

    assert len(grizzly.scenario.tasks()) == 1
    task = grizzly.scenario.tasks()[-1]

    assert isinstance(task, HttpClientTask)
    assert task.source == 'file-test.json'
    assert task.destination == 'uploaded-file-{{ suffix }}.json'
    assert task.endpoint == '{{ url }}'

    templates = task.get_templates()
    assert sorted(templates) == sorted(
        [
            '{{ url }}',
            'uploaded-file-{{ suffix }}.json',
        ],
    )

    test_file = grizzly_fixture.test_context / 'requests' / 'file-test.json'
    test_file.parent.mkdir(exist_ok=True)
    test_file.write_text('foobar {{ foo }}!')

    try:
        step_task_client_to_endpoint_file_destination(behave, request_method, 'file-test.json', 'http://{{ url }}', 'step-name', 'uploaded-file-{{ suffix }}.json')

        assert len(grizzly.scenario.tasks()) == 2
        task = grizzly.scenario.tasks()[-1]

        assert isinstance(task, HttpClientTask)
        assert task.source == 'foobar {{ foo }}!'
        assert task.destination == 'uploaded-file-{{ suffix }}.json'
        assert task.endpoint == '{{ url }}'

        templates = task.get_templates()
        assert sorted(templates) == sorted(
            [
                '{{ url }}',
                'uploaded-file-{{ suffix }}.json',
                'foobar {{ foo }}!',
            ],
        )
    finally:
        test_file.unlink()


@pytest.mark.parametrize('request_method', [RequestMethod.PUT, RequestMethod.POST])
def test_step_task_client_to_endpoint_text(grizzly_fixture: GrizzlyFixture, request_method: RequestMethod) -> None:
    behave = grizzly_fixture.behave.context
    grizzly = grizzly_fixture.grizzly
    grizzly.scenario.tasks.clear()

    assert len(grizzly.scenario.orphan_templates) == 0
    assert len(grizzly.scenario.tasks()) == 0

    behave.text = None
    step_task_client_to_endpoint_text(behave, request_method, 'http://example.org/put', 'step-name')
    assert behave.exceptions == {behave.scenario.name: [ANY(AssertionError, message='step text is mandatory for this step expression')]}
    delattr(behave, 'exceptions')

    behave.text = ''
    step_task_client_to_endpoint_text(behave, request_method, 'http://example.org/put', 'step-name')
    assert behave.exceptions == {behave.scenario.name: [ANY(AssertionError, message='step text cannot be an empty string')]}
    delattr(behave, 'exceptions')

    behave.text = 'foobar {{ foo }}!'

    step_task_client_to_endpoint_text(behave, request_method, 'http://{{ url }}', 'step-name')
    assert len(grizzly.scenario.tasks()) == 1
    task = grizzly.scenario.tasks()[-1]

    assert isinstance(task, HttpClientTask)
    assert task.source == 'foobar {{ foo }}!'
    assert task.destination is None
    assert task.endpoint == '{{ url }}'

    templates = task.get_templates()
    assert sorted(templates) == sorted(
        [
            '{{ url }}',
            'foobar {{ foo }}!',
        ],
    )
