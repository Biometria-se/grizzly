"""Unit tests of grizzly.steps.scenario.tasks.keystore."""

from __future__ import annotations

from typing import TYPE_CHECKING

from grizzly.steps import (
    step_task_keystore_decrement_default_with_step,
    step_task_keystore_get,
    step_task_keystore_get_default,
    step_task_keystore_get_remove,
    step_task_keystore_increment_default_with_step,
    step_task_keystore_pop,
    step_task_keystore_push,
    step_task_keystore_push_text,
    step_task_keystore_remove,
    step_task_keystore_set,
    step_task_keystore_set_text,
)
from grizzly.tasks import KeystoreTask

from test_framework.helpers import ANY

if TYPE_CHECKING:  # pragma: no cover
    from test_framework.fixtures import BehaveFixture


def test_step_task_keystore_get(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = behave_fixture.grizzly
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))

    grizzly.scenario.tasks.clear()

    grizzly.scenario.variables.update({'foobar': 'none'})

    step_task_keystore_get(behave, 'foobar', 'foobar')

    task = grizzly.scenario.tasks()[-1]

    assert isinstance(task, KeystoreTask)
    assert task.key == 'foobar'
    assert task.action == 'get'
    assert task.action_context == 'foobar'
    assert task.default_value is None


def test_step_task_keystore_get_remove(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = behave_fixture.grizzly
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))

    grizzly.scenario.tasks.clear()

    grizzly.scenario.variables.update({'foobar': 'none'})

    step_task_keystore_get_remove(behave, 'foobar', 'foobar')

    task = grizzly.scenario.tasks()[-1]

    assert isinstance(task, KeystoreTask)
    assert task.key == 'foobar'
    assert task.action == 'get_del'
    assert task.action_context == 'foobar'
    assert task.default_value is None


def test_step_task_keystore_get_default(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = behave_fixture.grizzly
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))
    behave.scenario = grizzly.scenario.behave

    grizzly.scenario.tasks.clear()

    grizzly.scenario.variables.update({'foobar': 'none'})

    step_task_keystore_get_default(behave, 'barfoo', 'foobar', "{'hello': 'world'}")

    task = grizzly.scenario.tasks()[-1]

    assert isinstance(task, KeystoreTask)
    assert task.key == 'barfoo'
    assert task.action == 'get'
    assert task.action_context == 'foobar'
    assert task.default_value == {'hello': 'world'}

    step_task_keystore_get_default(behave, 'barfoo', 'foobar', '"hello"')

    task = grizzly.scenario.tasks()[-1]

    assert isinstance(task, KeystoreTask)
    assert task.key == 'barfoo'
    assert task.action == 'get'
    assert task.action_context == 'foobar'
    assert task.default_value == 'hello'

    step_task_keystore_get_default(behave, 'barfoo', 'foobar', "'hello'")

    task = grizzly.scenario.tasks()[-1]

    assert isinstance(task, KeystoreTask)
    assert task.key == 'barfoo'
    assert task.action == 'get'
    assert task.action_context == 'foobar'
    assert task.default_value == 'hello'

    step_task_keystore_get_default(behave, 'barfoo', 'foobar', 'hello')
    assert behave.exceptions == {}


def test_step_task_keystore_set(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = behave_fixture.grizzly
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))
    behave.scenario = grizzly.scenario.behave

    grizzly.scenario.tasks.clear()

    step_task_keystore_set(behave, 'foobar', 'hello')
    assert behave.exceptions == {}
    delattr(behave, 'exceptions')

    step_task_keystore_set(behave, 'foobar', "'hello'")

    task = grizzly.scenario.tasks()[-1]

    assert getattr(behave, 'exceptions', {}) == {}
    assert isinstance(task, KeystoreTask)
    assert task.key == 'foobar'
    assert task.action == 'set'
    assert task.action_context == 'hello'
    assert task.arguments == {}

    step_task_keystore_set(behave, 'foobar', "['hello', 'world']")

    task = grizzly.scenario.tasks()[-1]

    assert getattr(behave, 'exceptions', {}) == {}
    assert isinstance(task, KeystoreTask)
    assert task.key == 'foobar'
    assert task.action == 'set'
    assert task.action_context == ['hello', 'world']
    assert task.arguments == {}

    step_task_keystore_set(behave, 'foobar', '{{ foo }} | render=True')

    task = grizzly.scenario.tasks()[-1]

    assert getattr(behave, 'exceptions', {}) == {}
    assert isinstance(task, KeystoreTask)
    assert task.key == 'foobar'
    assert task.action == 'set'
    assert task.action_context == '{{ foo }}'
    assert task.arguments == {'render': True}


def test_step_task_keystore_set_text(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = behave_fixture.grizzly
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))
    behave.scenario = grizzly.scenario.behave

    grizzly.scenario.tasks.clear()

    behave.text = 'hello'
    step_task_keystore_set_text(behave, 'foobar')
    assert behave.exceptions == {}
    delattr(behave, 'exceptions')

    behave.text = "'hello'"
    step_task_keystore_set_text(behave, 'foobar')

    task = grizzly.scenario.tasks()[-1]

    assert getattr(behave, 'exceptions', {}) == {}
    assert isinstance(task, KeystoreTask)
    assert task.key == 'foobar'
    assert task.action == 'set'
    assert task.action_context == 'hello'
    assert task.arguments == {}

    behave.text = "['hello', 'world']"
    step_task_keystore_set_text(behave, 'foobar')

    task = grizzly.scenario.tasks()[-1]

    assert getattr(behave, 'exceptions', {}) == {}
    assert isinstance(task, KeystoreTask)
    assert task.key == 'foobar'
    assert task.action == 'set'
    assert task.action_context == ['hello', 'world']
    assert task.arguments == {}

    behave.text = '{{ foo }} | render=True'
    step_task_keystore_set_text(behave, 'foobar')

    task = grizzly.scenario.tasks()[-1]

    assert getattr(behave, 'exceptions', {}) == {}
    assert isinstance(task, KeystoreTask)
    assert task.key == 'foobar'
    assert task.action == 'set'
    assert task.action_context == '{{ foo }}'
    assert task.arguments == {'render': True}


def test_step_task_keystore_increment_default_step_with_step(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = behave_fixture.grizzly
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))
    behave.scenario = grizzly.scenario.behave

    grizzly.scenario.tasks.clear()

    step_task_keystore_increment_default_with_step(behave, 'foobar', 1)
    assert behave.exceptions == {behave.scenario.name: [ANY(AssertionError, message='action context for "inc" must be a string')]}
    delattr(behave, 'exceptions')

    step_task_keystore_increment_default_with_step(behave, 'foobar', 'foobar')
    assert behave.exceptions == {behave.scenario.name: [ANY(AssertionError, message='variable "foobar" has not been initialized')]}
    delattr(behave, 'exceptions')

    grizzly.scenario.variables.update({'foobar': 'none'})
    step_task_keystore_increment_default_with_step(behave, 'foobar', 'foobar')

    task = grizzly.scenario.tasks()[-1]

    assert isinstance(task, KeystoreTask)
    assert task.key == 'foobar'
    assert task.action == 'inc'
    assert task.action_context == 'foobar'
    assert task.default_value is None


def test_step_task_keystore_decrement_default_step_with_step(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = behave_fixture.grizzly
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))
    behave.scenario = grizzly.scenario.behave

    grizzly.scenario.tasks.clear()

    step_task_keystore_decrement_default_with_step(behave, 'foobar', 1)
    assert behave.exceptions == {behave.scenario.name: [ANY(AssertionError, message='action context for "dec" must be a string')]}
    delattr(behave, 'exceptions')

    step_task_keystore_decrement_default_with_step(behave, 'foobar', 'foobar')
    assert behave.exceptions == {behave.scenario.name: [ANY(AssertionError, message='variable "foobar" has not been initialized')]}
    delattr(behave, 'exceptions')

    grizzly.scenario.variables.update({'foobar': 'none'})
    step_task_keystore_decrement_default_with_step(behave, 'foobar', 'foobar')

    task = grizzly.scenario.tasks()[-1]

    assert isinstance(task, KeystoreTask)
    assert task.key == 'foobar'
    assert task.action == 'dec'
    assert task.action_context == 'foobar'
    assert task.default_value is None


def test_step_task_keystore_pop(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = behave_fixture.grizzly

    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))
    behave.scenario = grizzly.scenario.behave

    grizzly.scenario.tasks.clear()
    grizzly.scenario.variables.clear()

    step_task_keystore_pop(behave, 'foobar', 1)
    assert behave.exceptions == {behave.scenario.name: [ANY(AssertionError, message='action context for "pop" must be a string')]}
    delattr(behave, 'exceptions')

    step_task_keystore_pop(behave, 'foobar', 'foobar')
    assert behave.exceptions == {behave.scenario.name: [ANY(AssertionError, message='variable "foobar" has not been initialized')]}
    delattr(behave, 'exceptions')

    grizzly.scenario.variables.update({'foobar': 'none'})
    step_task_keystore_pop(behave, 'foobar', 'foobar')

    task = grizzly.scenario.tasks()[-1]

    assert isinstance(task, KeystoreTask)
    assert task.key == 'foobar'
    assert task.action == 'pop'
    assert task.action_context == 'foobar'
    assert task.default_value is None


def test_step_task_keystore_push(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = behave_fixture.grizzly

    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))
    behave.scenario = grizzly.scenario.behave

    grizzly.scenario.tasks.clear()

    step_task_keystore_push(behave, 'foobar', 'hello')
    assert behave.exceptions == {}
    delattr(behave, 'exceptions')

    step_task_keystore_push(behave, 'foobar', "'hello'")

    task = grizzly.scenario.tasks()[-1]

    assert getattr(behave, 'exceptions', {}) == {}
    assert isinstance(task, KeystoreTask)
    assert task.key == 'foobar'
    assert task.action == 'push'
    assert task.action_context == 'hello'
    assert task.arguments == {}

    step_task_keystore_push(behave, 'foobar', "['hello', 'world']")

    task = grizzly.scenario.tasks()[-1]

    assert getattr(behave, 'exceptions', {}) == {}
    assert isinstance(task, KeystoreTask)
    assert task.key == 'foobar'
    assert task.action == 'push'
    assert task.action_context == ['hello', 'world']
    assert task.arguments == {}

    grizzly.scenario.variables.update({'hello': 'world'})
    step_task_keystore_push(behave, 'foobar', '{{ hello }} | render=True')

    assert getattr(behave, 'exceptions', {}) == {}

    task = grizzly.scenario.tasks()[-1]

    assert isinstance(task, KeystoreTask)
    assert task.key == 'foobar'
    assert task.action == 'push'
    assert task.action_context == '{{ hello }}'
    assert task.arguments == {'render': True}


def test_step_task_keystore_push_text(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = behave_fixture.grizzly

    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))
    behave.scenario = grizzly.scenario.behave

    grizzly.scenario.tasks.clear()

    behave.text = 'hello'
    step_task_keystore_push_text(behave, 'foobar')
    assert behave.exceptions == {}
    delattr(behave, 'exceptions')

    behave.text = "'hello'"
    step_task_keystore_push_text(behave, 'foobar')

    task = grizzly.scenario.tasks()[-1]

    assert getattr(behave, 'exceptions', {}) == {}
    assert isinstance(task, KeystoreTask)
    assert task.key == 'foobar'
    assert task.action == 'push'
    assert task.action_context == 'hello'
    assert task.arguments == {}

    behave.text = "['hello', 'world']"
    step_task_keystore_push_text(behave, 'foobar')

    task = grizzly.scenario.tasks()[-1]

    assert getattr(behave, 'exceptions', {}) == {}
    assert isinstance(task, KeystoreTask)
    assert task.key == 'foobar'
    assert task.action == 'push'
    assert task.action_context == ['hello', 'world']
    assert task.arguments == {}

    grizzly.scenario.variables.update({'hello': 'world'})
    behave.text = '{{ hello }} | render=True'
    step_task_keystore_push_text(behave, 'foobar')

    assert getattr(behave, 'exceptions', {}) == {}

    task = grizzly.scenario.tasks()[-1]

    assert isinstance(task, KeystoreTask)
    assert task.key == 'foobar'
    assert task.action == 'push'
    assert task.action_context == '{{ hello }}'
    assert task.arguments == {'render': True}


def test_step_task_keystore_remove(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = behave_fixture.grizzly

    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))
    behave.scenario = grizzly.scenario.behave

    grizzly.scenario.tasks.clear()

    step_task_keystore_remove(behave, 'foobar::{{ foo }}')

    task = grizzly.scenario.tasks()[-1]

    assert isinstance(task, KeystoreTask)
    assert task.key == 'foobar::{{ foo }}'
    assert task.action == 'del'
    assert task.action_context is None
