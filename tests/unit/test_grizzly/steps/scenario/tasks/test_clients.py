"""Unit tests of grizzly.steps.scenario.tasks.clients."""
from __future__ import annotations

from typing import TYPE_CHECKING, cast

import pytest

from grizzly.context import GrizzlyContext
from grizzly.steps import (
    step_task_client_get_endpoint_payload,
    step_task_client_get_endpoint_payload_metadata,
    step_task_client_put_endpoint_file,
    step_task_client_put_endpoint_file_destination,
)
from grizzly.tasks.clients import HttpClientTask
from grizzly.types import pymqi
from tests.helpers import ANY

if TYPE_CHECKING:  # pragma: no cover
    from tests.fixtures import BehaveFixture


def test_step_task_client_get_endpoint_payload_metadata(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast(GrizzlyContext, behave.grizzly)
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))
    behave.scenario = grizzly.scenario.behave

    step_task_client_get_endpoint_payload_metadata(behave, 'obscure.example.com', 'step-name', 'test', 'metadata')
    assert behave.exceptions == {behave.scenario.name: [ANY(AssertionError, message='could not find scheme in "obscure.example.com"')]}
    delattr(behave, 'exceptions')

    step_task_client_get_endpoint_payload_metadata(behave, 'obscure://obscure.example.com', 'step-name', 'test', 'metadata')
    assert behave.exceptions == {behave.scenario.name: [ANY(AssertionError, message='no client task registered for obscure')]}
    delattr(behave, 'exceptions')

    step_task_client_get_endpoint_payload_metadata(behave, 'http://www.example.org', 'step-name', 'test', 'metadata')
    assert behave.exceptions == {behave.scenario.name: [ANY(AssertionError, message='HttpClientTask: variable test has not been initialized')]}
    delattr(behave, 'exceptions')

    if pymqi.__name__ != 'grizzly_extras.dummy_pymqi':
        step_task_client_get_endpoint_payload_metadata(behave, 'mq://mq.example.org', 'step-name', 'test', 'metadata')
        assert behave.exceptions == {behave.scenario.name: [ANY(AssertionError, message='MessageQueueClientTask: variable test has not been initialized')]}
        delattr(behave, 'exceptions')

    grizzly.state.variables['test'] = 'none'

    step_task_client_get_endpoint_payload_metadata(behave, 'http://www.example.org', 'step-name', 'test', 'metadata')
    assert behave.exceptions == {behave.scenario.name: [ANY(AssertionError, message='HttpClientTask: variable metadata has not been initialized')]}
    delattr(behave, 'exceptions')

    if pymqi.__name__ != 'grizzly_extras.dummy_pymqi':
        step_task_client_get_endpoint_payload_metadata(behave, 'mq://mq.example.org', 'step-name', 'test', 'metadata')
        assert behave.exceptions == {behave.scenario.name: [ANY(AssertionError, message='MessageQueueClientTask: variable metadata has not been initialized')]}
        delattr(behave, 'exceptions')

    grizzly.state.variables['metadata'] = 'none'

    assert len(grizzly.scenario.tasks()) == 0
    step_task_client_get_endpoint_payload_metadata(behave, 'http://www.example.org', 'step-name', 'test', 'metadata')
    assert len(grizzly.scenario.tasks()) == 1

    grizzly.state.variables['endpoint_url'] = 'https://example.org'
    step_task_client_get_endpoint_payload_metadata(behave, 'https://{{ endpoint_url }}', 'step-name', 'test', 'metadata')

    task = grizzly.scenario.tasks()[-1]
    assert isinstance(task, HttpClientTask)
    assert task.endpoint == '{{ endpoint_url }}'
    assert sorted(task.get_templates()) == sorted(['{{ endpoint_url }}', '{{ test }} {{ metadata }}'])

    behave.text = '1=1'
    with pytest.raises(NotImplementedError, match='HttpClientTask has not implemented support for step text'):
        step_task_client_get_endpoint_payload_metadata(behave, 'https://{{ endpoint_url }}', 'step-name', 'test', 'metadata')
    assert behave.exceptions == {behave.scenario.name: [ANY(NotImplementedError, message='HttpClientTask has not implemented support for step text')]}
    delattr(behave, 'exceptions')


def test_step_task_client_get_endpoint_payload(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast(GrizzlyContext, behave.grizzly)
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))
    behave.scenario = grizzly.scenario.behave

    step_task_client_get_endpoint_payload(behave, 'obscure.example.com', 'step-name', 'test')
    assert behave.exceptions == {behave.scenario.name: [ANY(AssertionError, message='could not find scheme in "obscure.example.com"')]}
    delattr(behave, 'exceptions')

    step_task_client_get_endpoint_payload(behave, 'obscure://obscure.example.com', 'step-name', 'test')
    assert behave.exceptions == {behave.scenario.name: [ANY(AssertionError, message='no client task registered for obscure')]}
    delattr(behave, 'exceptions')

    step_task_client_get_endpoint_payload(behave, 'http://www.example.org', 'step-name', 'test')
    assert behave.exceptions == {behave.scenario.name: [ANY(AssertionError, message='HttpClientTask: variable test has not been initialized')]}
    delattr(behave, 'exceptions')

    if pymqi.__name__ != 'grizzly_extras.dummy_pymqi':
        step_task_client_get_endpoint_payload(behave, 'mq://mq.example.org', 'step-name', 'test')
        assert behave.exceptions == {behave.scenario.name: [ANY(AssertionError, message='MessageQueueClientTask: variable test has not been initialized')]}
        delattr(behave, 'exceptions')

    grizzly.state.variables['test'] = 'none'

    assert len(grizzly.scenario.tasks()) == 0
    step_task_client_get_endpoint_payload(behave, 'http://www.example.org', 'step-name', 'test')
    assert len(grizzly.scenario.tasks()) == 1

    grizzly.state.variables['endpoint_url'] = 'https://example.org'
    step_task_client_get_endpoint_payload(behave, 'https://{{ endpoint_url }}', 'step-name', 'test')

    task = grizzly.scenario.tasks()[-1]
    assert isinstance(task, HttpClientTask)
    assert task.endpoint == '{{ endpoint_url }}'
    assert sorted(task.get_templates()) == sorted(['{{ endpoint_url }}', '{{ test }}'])

    behave.text = '1=1'
    with pytest.raises(NotImplementedError, match='HttpClientTask has not implemented support for step text'):
        step_task_client_get_endpoint_payload(behave, 'https://{{ endpoint_url }}', 'step-name', 'test')
    assert behave.exceptions == {behave.scenario.name: [ANY(NotImplementedError, message='HttpClientTask has not implemented support for step text')]}
    delattr(behave, 'exceptions')


def test_step_task_client_put_endpoint_file(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast(GrizzlyContext, behave.grizzly)
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))
    behave.scenario = grizzly.scenario.behave

    behave.text = 'hello'

    assert len(grizzly.scenario.orphan_templates) == 0
    assert len(grizzly.scenario.tasks()) == 0

    step_task_client_put_endpoint_file(behave, 'file.json', 'http://example.org/put', 'step-name')
    for exceptions in behave.exceptions.values():
        for exception in exceptions:
            print(exception)
    assert behave.exceptions == {behave.scenario.name: [ANY(AssertionError, message='step text is not allowed for this step expression')]}
    delattr(behave, 'exceptions')

    behave.text = None

    step_task_client_put_endpoint_file(behave, 'file-{{ suffix }}.json', 'http://{{ url }}', 'step-name')
    assert behave.exceptions == {behave.scenario.name: [ANY(AssertionError, message='source file cannot be a template')]}
    delattr(behave, 'exceptions')

    step_task_client_put_endpoint_file(behave, 'file-test.json', 'http://{{ url }}', 'step-name')

    assert len(grizzly.scenario.tasks()) == 1
    task = grizzly.scenario.tasks()[-1]

    assert isinstance(task, HttpClientTask)
    assert task.source == 'file-test.json'
    assert task.destination is None
    assert task.endpoint == '{{ url }}'

    templates = task.get_templates()
    assert len(templates) == 1
    assert sorted(templates) == sorted([
        '{{ url }}',
    ])


def test_step_task_client_put_endpoint_file_destination(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast(GrizzlyContext, behave.grizzly)
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))
    behave.scenario = grizzly.scenario.behave

    behave.text = 'hello'

    assert len(grizzly.scenario.orphan_templates) == 0
    assert len(grizzly.scenario.tasks()) == 0

    step_task_client_put_endpoint_file_destination(behave, 'file.json', 'http://example.org/put', 'step-name', 'uploaded-file.json')
    assert behave.exceptions == {behave.scenario.name: [ANY(AssertionError, message='step text is not allowed for this step expression')]}
    delattr(behave, 'exceptions')

    behave.text = None

    step_task_client_put_endpoint_file_destination(behave, 'file-{{ suffix }}.json', 'http://{{ url }}', 'step-name', 'uploaded-file-{{ suffix }}.json')
    assert behave.exceptions == {behave.scenario.name: [ANY(AssertionError, message='source file cannot be a template')]}
    delattr(behave, 'exceptions')

    step_task_client_put_endpoint_file_destination(behave, 'file-test.json', 'http://{{ url }}', 'step-name', 'uploaded-file-{{ suffix }}.json')

    assert len(grizzly.scenario.tasks()) == 1
    task = grizzly.scenario.tasks()[-1]

    assert isinstance(task, HttpClientTask)
    assert task.source == 'file-test.json'
    assert task.destination == 'uploaded-file-{{ suffix }}.json'
    assert task.endpoint == '{{ url }}'

    templates = task.get_templates()
    assert len(templates) == 2
    assert sorted(templates) == sorted([
        '{{ url }}',
        'uploaded-file-{{ suffix }}.json',
    ])
