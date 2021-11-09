import pytest

from parse import compile
from behave.runner import Context

from grizzly.steps import *  # pylint: disable=unused-wildcard-import

from ...fixtures import behave_context, locust_environment  # pylint: disable=unused-import


def test_parse_user_gramatical_number() -> None:
    p = compile(
        'we have {user:d} {user_number:UserGramaticalNumber}',
        extra_types=dict(UserGramaticalNumber=parse_user_gramatical_number),
    )

    assert p.parse('we have 1 user')['user_number'] == 'user'
    assert p.parse('we have 2 users')['user_number'] == 'users'
    assert p.parse('we have 3 people') is None


@pytest.mark.usefixtures('behave_context')
def test_step_shapes_user_count(behave_context: Context) -> None:
    step_impl = step_shapes_user_count

    grizzly = cast(GrizzlyContext, behave_context.grizzly)
    assert grizzly.setup.user_count == 0

    with pytest.raises(AssertionError):
        step_impl(behave_context, 10, 'user')

    step_impl(behave_context, 10, 'users')

    assert grizzly.setup.user_count == 10

    step_impl(behave_context, 1, 'user')

    assert grizzly.setup.user_count == 1

    with pytest.raises(AssertionError):
        step_impl(behave_context, 1, 'users')

    grizzly.setup.spawn_rate = 10

    with pytest.raises(AssertionError):
        step_impl(behave_context, 1, 'user')


@pytest.mark.usefixtures('behave_context')
def test_step_shapes_spawn_rate(behave_context: Context) -> None:
    step_impl = step_shapes_spawn_rate

    grizzly = cast(GrizzlyContext, behave_context.grizzly)
    assert grizzly.setup.spawn_rate is None

    # spawn_rate must be <= user_count
    with pytest.raises(AssertionError):
        step_impl(behave_context, 1, 'user')

    grizzly.setup.user_count = 10
    step_impl(behave_context, 0.1, 'users')
    assert grizzly.setup.spawn_rate == 0.1

    step_impl(behave_context, 10, 'users')
    assert grizzly.setup.spawn_rate == 10

    grizzly.setup.user_count = 1

    with pytest.raises(AssertionError):
        step_impl(behave_context, 10, 'users')
