"""End-to-end tests for grizzly.steps.background.shapes."""
from __future__ import annotations

from contextlib import suppress
from typing import TYPE_CHECKING, Dict, List, Optional, cast

import pytest

from grizzly.context import GrizzlyContext
from grizzly.utils import is_template

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.types.behave import Context
    from tests.fixtures import End2EndFixture


@pytest.mark.parametrize('count', [
    '5', '1', "{{ user_count }}",
])
def test_e2e_step_shapes_user_count(e2e_fixture: End2EndFixture, count: str) -> None:
    def validator(context: Context) -> None:
        grizzly = cast(GrizzlyContext, context.grizzly)
        data = next(iter(context.table)).as_dict()

        user_count = int(data['user_count'].replace('{{ user_count }}', '10'))

        assert grizzly.setup.user_count == user_count, f'{grizzly.setup.user_count} != {user_count}'

    table: List[Dict[str, str]] = [
        {
            'user_count': str(count),
        },
    ]

    suffix = 's'
    with suppress(Exception):
        if int(count) <= 1:
            suffix = ''

    e2e_fixture.add_validator(validator, table=table)

    background: List[str] = []
    testdata: Optional[Dict[str, str]] = None

    if is_template(count):
        background.append('Then ask for value of variable "user_count"')
        testdata = {'user_count': '10'}

    feature_file = e2e_fixture.test_steps(
        background=[
            *background,
            f'Given "{count}" user{suffix}',
        ],
        scenario=[
            f'And repeat for "{count}" iteration{suffix}',
        ],
        identifier=count,
    )

    rc, _ = e2e_fixture.execute(feature_file, testdata=testdata)

    assert rc == 0


@pytest.mark.parametrize('rate', [
    '1', '0.5', "{{ spawn_rate }}",
])
def test_e2e_step_shapes_spawn_rate(e2e_fixture: End2EndFixture, rate: str) -> None:
    def validator(context: Context) -> None:
        grizzly = cast(GrizzlyContext, context.grizzly)
        data = next(iter(context.table)).as_dict()

        spawn_rate = float(data['spawn_rate'].replace('{{ spawn_rate }}', '0.01'))

        assert grizzly.setup.spawn_rate == spawn_rate, f'{grizzly.setup.spawn_rate} != {spawn_rate}'

    table: List[Dict[str, str]] = [
        {
            'spawn_rate': str(rate),
        },
    ]

    e2e_fixture.add_validator(validator, table=table)

    background: List[str] = []
    testdata: Optional[Dict[str, str]] = None

    if '{{' in rate and '}}' in rate:
        background.append('Then ask for value of variable "spawn_rate"')
        testdata = {'spawn_rate': '0.001'}

    feature_file = e2e_fixture.test_steps(
        background=[
            *background,
            f'Given spawn rate is "{rate}" users per second',
        ],
        identifier=rate,
    )

    rc, _ = e2e_fixture.execute(feature_file, testdata=testdata)

    assert rc == 0
