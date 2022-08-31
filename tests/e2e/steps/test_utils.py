from ...fixtures import End2EndFixture


def test_e2e_step_utils_fail(e2e_fixture: End2EndFixture) -> None:
    feature_file = e2e_fixture.test_steps(
        scenario=[
            'Then fail'
        ]
    )

    rc, output = e2e_fixture.execute(feature_file)
    result = ''.join(output)

    assert rc == 1
    assert 'Then log message "dummy"                                         # None' in result
    assert '''0 features passed, 1 failed, 0 skipped
0 scenarios passed, 1 failed, 0 skipped
3 steps passed, 1 failed, 1 skipped, 0 undefined''' in result
