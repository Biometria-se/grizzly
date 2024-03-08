"""End-to-end tests of grizzly.steps.utils."""
from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:  # pragma: no cover
    from tests.fixtures import End2EndFixture


def test_e2e_step_utils_fail(e2e_fixture: End2EndFixture) -> None:
    if e2e_fixture._distributed:
        pytest.skip('this step executes before grizzly has started locust and cannot run distributed')

    feature_file = e2e_fixture.test_steps(
        scenario=[
            'Then fail',
        ],
    )

    rc, output = e2e_fixture.execute(feature_file)
    result = ''.join(output)

    assert rc == 1
    assert """Failure summary:
    Scenario: test_e2e_step_utils_fail
        Then fail # features/test_e2e_step_utils_fail.lock.feature:8
            ! manually failed""" in result
    assert """0 features passed, 1 failed, 0 skipped
0 scenarios passed, 1 failed, 0 skipped
4 steps passed, 1 failed, 0 skipped, 0 undefined""" in result
