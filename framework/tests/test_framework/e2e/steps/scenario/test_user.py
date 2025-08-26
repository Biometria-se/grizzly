"""End-to-end tests of grizzly.steps.scenario.user."""

from __future__ import annotations

from secrets import choice
from typing import TYPE_CHECKING, cast

import pytest

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.context import GrizzlyContext
    from grizzly.types.behave import Context

    from test_framework.fixtures import End2EndFixture


@pytest.mark.parametrize(
    ('user_type', 'host', 'expected_rc'),
    [
        ('RestApi', 'https://localhost/api', 0),
        ('MessageQueueUser', 'mq://localhost/?QueueManager=QMGR01&Channel=Channel01', 1),
        ('ServiceBus', 'sb://localhost/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=abc123def456ghi789=', 0),
        ('BlobStorageUser', 'DefaultEndpointsProtocol=https;EndpointSuffix=localhost;AccountName=examplestorage;AccountKey=xxxyyyyzzz==', 0),
    ],
)
def test_e2e_step_user_type_count_tag(e2e_fixture: End2EndFixture, user_type: str, host: str, expected_rc: int) -> None:
    def validate_user_type(context: Context) -> None:
        from grizzly.locust import FixedUsersDispatcher

        grizzly = cast('GrizzlyContext', context.grizzly)
        data = next(iter(context.table)).as_dict()

        expected_user_type = data['user_type']
        expected_user_count = int(data['user_count'])
        expected_tag = data['tag']
        expected_host = data['host']
        actual_host = grizzly.scenario.context.get('host', None)

        if not expected_user_type.endswith('User'):
            expected_user_type += 'User'

        assert grizzly.setup.dispatcher_class == FixedUsersDispatcher
        assert grizzly.scenario.user.class_name == expected_user_type
        assert grizzly.scenario.user.sticky_tag == expected_tag
        assert grizzly.scenario.user.fixed_count == expected_user_count
        assert grizzly.scenario.user.weight == 1
        assert actual_host == expected_host, f'{actual_host} != {expected_host}'

    if 'messagequeue' in user_type.lower() and not e2e_fixture.has_pymqi():
        pytest.skip('pymqi not installed')

    host = host.replace('localhost', e2e_fixture.host)

    user_count = choice(range(1, 20))
    user_tag = choice(['foo', 'bar', 'hello', 'world'])
    grammar = choice(['user', 'users'])

    table: list[dict[str, str]] = [
        {
            'user_type': user_type,
            'user_count': str(user_count),
            'tag': user_tag,
            'host': host,
        },
    ]

    e2e_fixture.add_validator(validate_user_type, table=table)

    feature_file = e2e_fixture.test_steps(
        scenario=[
            f'Given "{user_count}" {grammar} of type "{user_type}" with tag "{user_tag}" load testing "{host}"',
            'And set context variable "auth.username" to "grizzly"',
            'And set context variable "auth.password" to "locust"',
            f'And repeat for "{user_count}" iterations',
        ],
        identifier=user_type,
    )

    rc, output = e2e_fixture.execute(feature_file)

    try:
        assert rc == expected_rc
    except AssertionError:
        print(''.join(output))
        raise


@pytest.mark.parametrize(
    ('user_type', 'host', 'expected_rc'),
    [
        ('RestApi', 'https://localhost/api', 0),
        ('MessageQueueUser', 'mq://localhost/?QueueManager=QMGR01&Channel=Channel01', 1),
        ('ServiceBus', 'sb://localhost/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=abc123def456ghi789=', 0),
        ('BlobStorageUser', 'DefaultEndpointsProtocol=https;EndpointSuffix=localhost;AccountName=examplestorage;AccountKey=xxxyyyyzzz==', 0),
    ],
)
def test_e2e_step_user_type_with_weight(e2e_fixture: End2EndFixture, user_type: str, host: str, expected_rc: int) -> None:
    def validate_user_type(context: Context) -> None:
        grizzly = cast('GrizzlyContext', context.grizzly)
        data = next(iter(context.table)).as_dict()

        expected_weight = int(data['weight'])
        expected_user_type = data['user_type']
        expected_host = data['host']
        actual_host = grizzly.scenario.context.get('host', None)

        if not expected_user_type.endswith('User'):
            expected_user_type += 'User'

        assert grizzly.scenario.user.class_name == expected_user_type
        assert grizzly.scenario.user.weight == expected_weight
        assert actual_host == expected_host, f'{actual_host} != {expected_host}'

    if 'messagequeue' in user_type.lower() and not e2e_fixture.has_pymqi():
        pytest.skip('pymqi not installed')

    host = host.replace('localhost', e2e_fixture.host)

    weight = choice(range(1, 20))

    table: list[dict[str, str]] = [
        {
            'user_type': user_type,
            'weight': str(weight),
            'host': host,
        },
    ]

    e2e_fixture.add_validator(validate_user_type, table=table)

    feature_file = e2e_fixture.test_steps(
        scenario=[
            f'Given a user of type "{user_type}" with weight "{weight}" load testing "{host}"',
            'And set context variable "auth.username" to "grizzly"',
            'And set context variable "auth.password" to "locust"',
        ],
        identifier=user_type,
    )

    rc, _ = e2e_fixture.execute(feature_file)

    assert rc == expected_rc


@pytest.mark.parametrize(
    ('user_type', 'host', 'expected_rc'),
    [
        ('RestApi', 'https://localhost/api', 0),
        ('MessageQueueUser', 'mq://localhost/?QueueManager=QMGR01&Channel=Channel01', 1),
        ('ServiceBus', 'sb://localhost/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=abc123def456ghi789=', 0),
        ('BlobStorageUser', 'DefaultEndpointsProtocol=https;EndpointSuffix=localhost;AccountName=examplestorage;AccountKey=xxxyyyyzzz==', 0),
    ],
)
def test_e2e_step_user_type(e2e_fixture: End2EndFixture, user_type: str, host: str, expected_rc: int) -> None:
    def validate_user_type(context: Context) -> None:
        grizzly = cast('GrizzlyContext', context.grizzly)
        data = next(iter(context.table)).as_dict()

        expected_user_type = data['user_type']
        expected_host = data['host']

        if not expected_user_type.endswith('User'):
            expected_user_type += 'User'

        assert grizzly.scenario.user.class_name == expected_user_type
        assert grizzly.scenario.user.weight == 1
        assert grizzly.scenario.context.get('host', None) == expected_host

    if 'messagequeue' in user_type.lower() and not e2e_fixture.has_pymqi():
        pytest.skip('pymqi not installed')

    host = host.replace('localhost', e2e_fixture.host)

    table: list[dict[str, str]] = [
        {
            'user_type': user_type,
            'host': host,
        },
    ]

    e2e_fixture.add_validator(validate_user_type, table=table)

    feature_file = e2e_fixture.test_steps(
        scenario=[
            f'Given a user of type "{user_type}" load testing "{host}"',
            'And set context variable "auth.username" to "grizzly"',
            'And set context variable "auth.password" to "locust"',
        ],
        identifier=user_type,
    )

    rc, output = e2e_fixture.execute(feature_file)

    try:
        assert rc == expected_rc
    except AssertionError:
        print(''.join(output))
        raise
