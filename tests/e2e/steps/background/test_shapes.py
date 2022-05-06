from typing import cast, List, Dict

import pytest

from behave.runner import Context
from grizzly.context import GrizzlyContext

from ....fixtures import BehaveContextFixture


@pytest.mark.parametrize('count', [
    '5', '1', "{{ user_count }}",
])
def test_e2e_step_shapes_user_count(behave_context_fixture: BehaveContextFixture, count: str) -> None:
    def validate_user_count(context: Context) -> None:
        grizzly = cast(GrizzlyContext, context.grizzly)
        data = list(context.table)[0].as_dict()

        user_count = int(data['user_count'].replace('{{ user_count }}', '10'))

        assert grizzly.setup.user_count == user_count, f'{grizzly.setup.user_count} != {user_count}'

        raise SystemExit(0)

    table: List[Dict[str, str]] = [
        {
            'user_count': str(count),
        }
    ]

    suffix = 's'
    try:
        if int(count) <= 1:
            suffix = ''
    except:
        pass

    behave_context_fixture.add_validator(validate_user_count, table=table)

    feature_file = behave_context_fixture.test_steps(
        background=[
            'Then ask for value of variable "user_count"',
            f'Given "{count}" user{suffix}',
        ],
        scenario=[
            f'And repeat for "{count}" iteration{suffix}',
        ],
        identifier=count,
    )

    rc, _ = behave_context_fixture.execute(feature_file, testdata={'user_count': '10'})

    assert rc == 0


@pytest.mark.parametrize('rate', [
    '1', '0.5', "{{ spawn_rate }}",
])
def test_e2e_step_shapes_spawn_rate(behave_context_fixture: BehaveContextFixture, rate: str) -> None:
    def validate_spawn_rate(context: Context) -> None:
        grizzly = cast(GrizzlyContext, context.grizzly)
        data = list(context.table)[0].as_dict()

        spawn_rate = float(data['spawn_rate'].replace('{{ spawn_rate }}', '0.01'))

        assert grizzly.setup.spawn_rate == spawn_rate, f'{grizzly.setup.spawn_rate} != {spawn_rate}'

        raise SystemExit(0)

    table: List[Dict[str, str]] = [
        {
            'spawn_rate': str(rate),
        }
    ]

    behave_context_fixture.add_validator(validate_spawn_rate, table=table)

    feature_file = behave_context_fixture.test_steps(
        background=[
            'Then ask for value of variable "spawn_rate"',
            f'Given spawn rate is "{rate}" users per second',
        ],
        identifier=rate,
    )

    rc, _ = behave_context_fixture.execute(feature_file, testdata={'spawn_rate': '0.001'})

    assert rc == 0
