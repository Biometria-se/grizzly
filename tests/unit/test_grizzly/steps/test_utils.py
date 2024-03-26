"""Unit tests of grizzly.tests.utils."""
from __future__ import annotations

from typing import TYPE_CHECKING

from grizzly.steps.utils import step_utils_add_orphan_template, step_utils_fail
from tests.helpers import ANY

if TYPE_CHECKING:  # pragma: no cover
    from tests.fixtures import BehaveFixture


def test_step_utils_fail(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    assert behave.exceptions == {}

    step_utils_fail(behave)

    assert behave.exceptions == {behave.scenario.name: [ANY(AssertionError, message='manually failed')]}

def test_step_utils_add_orphan_template(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = behave_fixture.grizzly
    grizzly.scenarios.create(behave_fixture.create_scenario('test'))

    assert grizzly.scenario.orphan_templates == []

    step_utils_add_orphan_template(behave, '{{ hello world }}')

    assert grizzly.scenario.orphan_templates == ['{{ hello world }}']
