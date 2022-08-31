from typing import cast, List, Dict
from random import randint

import pytest

from behave.runner import Context
from grizzly.context import GrizzlyContext

from ....fixtures import End2EndFixture


@pytest.mark.parametrize('user_type,host', [
    ('RestApi', 'https://api.example.com',),
    ('MessageQueueUser', 'mq://mqm:secret@mq.example.com/?QueueManager=QMGR01&Channel=Channel01',),
    ('ServiceBus', 'sb://sb.example.com/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=abc123def456ghi789=',),
    ('BlobStorageUser', 'DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net;AccountName=examplestorage;AccountKey=xxxyyyyzzz==',),
    ('Sftp', 'sftp://ftp.example.com',),
])
def test_e2e_step_user_type_with_weight(e2e_fixture: End2EndFixture, user_type: str, host: str) -> None:
    def validate_user_type(context: Context) -> None:
        grizzly = cast(GrizzlyContext, context.grizzly)
        data = list(context.table)[0].as_dict()

        expected_weight = int(data['weight'])
        expected_user_type = data['user_type']
        expected_host = data['host']

        if not expected_user_type.endswith('User'):
            expected_user_type += 'User'

        assert grizzly.scenario.user.class_name == expected_user_type
        assert grizzly.scenario.user.weight == expected_weight
        assert grizzly.scenario.context.get('host', None) == expected_host

    weight = randint(1, 100)

    table: List[Dict[str, str]] = [{
        'user_type': user_type,
        'weight': str(weight),
        'host': host,
    }]

    e2e_fixture.add_validator(validate_user_type, table=table)

    feature_file = e2e_fixture.test_steps(
        scenario=[
            f'Given a user of type "{user_type}" with weight "{weight}" load testing "{host}"',
        ],
        identifier=user_type,
    )

    rc, _ = e2e_fixture.execute(feature_file)

    assert rc == 0


@pytest.mark.parametrize('user_type,host', [
    ('RestApi', 'https://api.example.com',),
    ('MessageQueueUser', 'mq://mqm:secret@mq.example.com/?QueueManager=QMGR01&Channel=Channel01',),
    ('ServiceBus', 'sb://sb.example.com/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=abc123def456ghi789=',),
    ('BlobStorageUser', 'DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net;AccountName=examplestorage;AccountKey=xxxyyyyzzz==',),
    ('Sftp', 'sftp://ftp.example.com',),
])
def test_e2e_step_user_type(e2e_fixture: End2EndFixture, user_type: str, host: str) -> None:
    def validate_user_type(context: Context) -> None:
        grizzly = cast(GrizzlyContext, context.grizzly)
        data = list(context.table)[0].as_dict()

        expected_user_type = data['user_type']
        expected_host = data['host']

        if not expected_user_type.endswith('User'):
            expected_user_type += 'User'

        assert grizzly.scenario.user.class_name == expected_user_type
        assert grizzly.scenario.user.weight == 1
        assert grizzly.scenario.context.get('host', None) == expected_host

    table: List[Dict[str, str]] = [{
        'user_type': user_type,
        'host': host,
    }]

    e2e_fixture.add_validator(validate_user_type, table=table)

    feature_file = e2e_fixture.test_steps(
        scenario=[
            f'Given a user of type "{user_type}" load testing "{host}"',
        ],
        identifier=user_type,
    )

    rc, _ = e2e_fixture.execute(feature_file)

    assert rc == 0
