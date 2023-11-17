from typing import cast

import pytest

from grizzly.steps import (
    step_task_client_get_endpoint_payload_metadata,
    step_task_client_get_endpoint_payload,
    step_task_client_put_endpoint_file,
    step_task_client_put_endpoint_file_destination,
)
from grizzly.context import GrizzlyContext
from grizzly.tasks.clients import HttpClientTask
from grizzly.types import pymqi
from tests.fixtures import BehaveFixture


def test_step_task_client_get_endpoint_payload_metadata(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast(GrizzlyContext, behave.grizzly)
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))

    with pytest.raises(AssertionError) as ae:
        step_task_client_get_endpoint_payload_metadata(behave, 'obscure.example.com', 'step-name', 'test', 'metadata')
    assert 'could not find scheme in "obscure.example.com"' in str(ae)

    with pytest.raises(AssertionError) as ae:
        step_task_client_get_endpoint_payload_metadata(behave, 'obscure://obscure.example.com', 'step-name', 'test', 'metadata')
    assert 'no client task registered for obscure' in str(ae)

    with pytest.raises(ValueError) as ve:
        step_task_client_get_endpoint_payload_metadata(behave, 'http://www.example.org', 'step-name', 'test', 'metadata')
    assert 'HttpClientTask: variable test has not been initialized' in str(ve)

    if pymqi.__name__ != 'grizzly_extras.dummy_pymqi':
        with pytest.raises(ValueError) as ve:
            step_task_client_get_endpoint_payload_metadata(behave, 'mq://mq.example.org', 'step-name', 'test', 'metadata')
        assert 'MessageQueueClientTask: variable test has not been initialized' in str(ve)

    grizzly.state.variables['test'] = 'none'

    with pytest.raises(ValueError) as ve:
        step_task_client_get_endpoint_payload_metadata(behave, 'http://www.example.org', 'step-name', 'test', 'metadata')
    assert 'HttpClientTask: variable metadata has not been initialized' in str(ve)

    if pymqi.__name__ != 'grizzly_extras.dummy_pymqi':
        with pytest.raises(ValueError) as ve:
            step_task_client_get_endpoint_payload_metadata(behave, 'mq://mq.example.org', 'step-name', 'test', 'metadata')
        assert 'MessageQueueClientTask: variable metadata has not been initialized' in str(ve)

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
    with pytest.raises(NotImplementedError) as nie:
        step_task_client_get_endpoint_payload_metadata(behave, 'https://{{ endpoint_url }}', 'step-name', 'test', 'metadata')
    assert str(nie.value) == 'HttpClientTask has not implemented support for step text'


def test_step_task_client_get_endpoint_payload(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast(GrizzlyContext, behave.grizzly)
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))

    with pytest.raises(AssertionError) as ae:
        step_task_client_get_endpoint_payload(behave, 'obscure.example.com', 'step-name', 'test')
    assert 'could not find scheme in "obscure.example.com"' in str(ae)

    with pytest.raises(AssertionError) as ae:
        step_task_client_get_endpoint_payload(behave, 'obscure://obscure.example.com', 'step-name', 'test')
    assert 'no client task registered for obscure' in str(ae)

    with pytest.raises(ValueError) as ve:
        step_task_client_get_endpoint_payload(behave, 'http://www.example.org', 'step-name', 'test')
    assert 'HttpClientTask: variable test has not been initialized' in str(ve)

    if pymqi.__name__ != 'grizzly_extras.dummy_pymqi':
        with pytest.raises(ValueError) as ve:
            step_task_client_get_endpoint_payload(behave, 'mq://mq.example.org', 'step-name', 'test')
        assert 'MessageQueueClientTask: variable test has not been initialized' in str(ve)

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
    with pytest.raises(NotImplementedError) as nie:
        step_task_client_get_endpoint_payload(behave, 'https://{{ endpoint_url }}', 'step-name', 'test')
    assert str(nie.value) == 'HttpClientTask has not implemented support for step text'


def test_step_task_client_put_endpoint_file(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast(GrizzlyContext, behave.grizzly)
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))

    behave.text = 'hello'

    assert len(grizzly.scenario.orphan_templates) == 0
    assert len(grizzly.scenario.tasks()) == 0

    with pytest.raises(AssertionError) as ae:
        step_task_client_put_endpoint_file(behave, 'file.json', 'http://example.org/put', 'step-name')
    assert 'step text is not allowed for this step expression' in str(ae.value)

    behave.text = None

    with pytest.raises(AssertionError) as ae:
        step_task_client_put_endpoint_file(behave, 'file-{{ suffix }}.json', 'http://{{ url }}', 'step-name')
    assert 'source file cannot be a template' == str(ae.value)

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

    behave.text = 'hello'

    assert len(grizzly.scenario.orphan_templates) == 0
    assert len(grizzly.scenario.tasks()) == 0

    with pytest.raises(AssertionError) as ae:
        step_task_client_put_endpoint_file_destination(behave, 'file.json', 'http://example.org/put', 'step-name', 'uploaded-file.json')
    assert 'step text is not allowed for this step expression' in str(ae.value)

    behave.text = None

    with pytest.raises(AssertionError) as ae:
        step_task_client_put_endpoint_file_destination(behave, 'file-{{ suffix }}.json', 'http://{{ url }}', 'step-name', 'uploaded-file-{{ suffix }}.json')
    assert 'source file cannot be a template' == str(ae.value)

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
