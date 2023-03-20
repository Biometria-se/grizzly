import re
import pytest

from tests.fixtures import End2EndFixture


def test_e2e_step_utils_fail(e2e_fixture: End2EndFixture) -> None:
    if e2e_fixture._distributed:
        pytest.skip('this step executes before grizzly has started locust and cannot run distributed')

    feature_file = e2e_fixture.test_steps(
        scenario=[
            'Then fail'
        ]
    )

    rc, output = e2e_fixture.execute(feature_file)
    result = ''.join(output)

    assert rc == 1
    assert re.search(r'\s+Then log message "dummy"\s+# None', result, re.MULTILINE)
    assert '''0 features passed, 1 failed, 0 skipped
0 scenarios passed, 1 failed, 0 skipped
3 steps passed, 1 failed, 1 skipped, 0 undefined''' in result
