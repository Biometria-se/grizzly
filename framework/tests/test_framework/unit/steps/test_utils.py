"""Unit tests of grizzly.tests.utils."""

from __future__ import annotations

from typing import TYPE_CHECKING

from grizzly.steps.utils import step_utils_add_orphan_template, step_utils_fail_scenario

from test_framework.helpers import ANY

if TYPE_CHECKING:  # pragma: no cover
    from test_framework.fixtures import BehaveFixture


def test_step_utils_fail(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    assert behave.exceptions == {}

    step_utils_fail_scenario(behave)

    assert behave.exceptions == {behave.scenario.name: [ANY(AssertionError, message='manually failed')]}


def test_step_utils_add_orphan_template(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = behave_fixture.grizzly

    test_scenario = grizzly.scenarios.create(behave_fixture.create_scenario('test'))
    grizzly.scenarios.create(behave_fixture.create_scenario('second'))
    grizzly.scenarios.create(behave_fixture.create_scenario('third'))
    grizzly.scenarios.select(test_scenario.behave)

    behave_fixture.create_step('test step', in_background=False, context=behave)

    assert grizzly.scenario.orphan_templates == []

    step_utils_add_orphan_template(behave, '{{ hello world }}')

    assert grizzly.scenario.orphan_templates == ['{{ hello world }}']

    for scenario in grizzly.scenarios:
        if scenario is test_scenario:
            continue

        assert scenario.orphan_templates == []

    test_scenario.orphan_templates.clear()

    behave_fixture.create_step('test step background', in_background=True, context=behave)

    step_utils_add_orphan_template(behave, '{{ foobar }}')

    for scenario in grizzly.scenarios:
        assert scenario.orphan_templates == ['{{ foobar }}']
