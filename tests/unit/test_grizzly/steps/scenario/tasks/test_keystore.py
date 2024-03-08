"""Unit tests of grizzly.steps.scenario.tasks.keystore."""
from __future__ import annotations

from typing import TYPE_CHECKING

from grizzly.steps import step_task_keystore_get, step_task_keystore_get_default, step_task_keystore_set
from grizzly.tasks import KeystoreTask
from tests.helpers import ANY

if TYPE_CHECKING:  # pragma: no cover
    from tests.fixtures import BehaveFixture


def test_step_task_keystore_get(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = behave_fixture.grizzly
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))

    grizzly.scenario.tasks.clear()

    grizzly.state.variables.update({'foobar': 'none'})

    step_task_keystore_get(behave, 'foobar', 'foobar')

    task = grizzly.scenario.tasks()[-1]

    assert isinstance(task, KeystoreTask)
    assert task.key == 'foobar'
    assert task.action == 'get'
    assert task.action_context == 'foobar'
    assert task.default_value is None


def test_step_task_keystore_get_default(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = behave_fixture.grizzly
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))
    behave.scenario = grizzly.scenario.behave

    grizzly.scenario.tasks.clear()

    grizzly.state.variables.update({'foobar': 'none'})

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
    assert behave.exceptions == {behave.scenario.name: [ANY(AssertionError, message='"hello" is not valid JSON')]}

def test_step_task_keystore_set(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = behave_fixture.grizzly
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))
    behave.scenario = grizzly.scenario.behave

    grizzly.scenario.tasks.clear()

    step_task_keystore_set(behave, 'foobar', 'hello')
    assert behave.exceptions == {behave.scenario.name: [ANY(AssertionError, message='"hello" is not valid JSON')]}

    step_task_keystore_set(behave, 'foobar', "'hello'")

    task = grizzly.scenario.tasks()[-1]

    assert isinstance(task, KeystoreTask)
    assert task.key == 'foobar'
    assert task.action == 'set'
    assert task.action_context == 'hello'

    step_task_keystore_set(behave, 'foobar', "['hello', 'world']")

    task = grizzly.scenario.tasks()[-1]

    assert isinstance(task, KeystoreTask)
    assert task.key == 'foobar'
    assert task.action == 'set'
    assert task.action_context == ['hello', 'world']
