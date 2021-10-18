from typing import Any

import pymqi

from pytest_mock import mocker  # pylint: disable=unused-import
from pytest_mock.plugin import MockerFixture
class TestMessageQueueIntegration:
    def test___init__(self) -> None:
        pass

class TestMessageQueueDaemon:
    def test_router(self, mocker: MockerFixture) -> None:
        def mocked_pymqi_connect(queue_manager: str, channel: str, conn_info: str, username: str, password: str) -> Any:
            raise pymqi.MQMIError(comp=2, reason=2538)

        mocker.patch(
            'pymqi.connect',
            mocked_pymqi_connect,
        )

        def mocked_connect_with_options(i: pymqi.QueueManager, user: bytes, password: bytes, cd: pymqi.CD, sco: pymqi.SCO) -> None:
            assert user == 'test_username'.encode('utf-8')
            assert password == 'test_password'.encode('utf-8')

            assert cd.ChannelName == 'Kanal1'.encode('utf-8')
            assert cd.ConnectionName == 'mq.example.com(1337)'.encode('utf-8')
            assert cd.ChannelType == pymqi.CMQC.MQCHT_CLNTCONN
            assert cd.TransportType == pymqi.CMQC.MQXPT_TCP
            assert cd.SSLCipherSpec == 'ECDHE_RSA_AES_256_GCM_SHA384'

            assert sco.KeyRepository == '/home/test/key_file'.encode('utf-8')
            assert sco.CertificateLabel == 'test_cert_label'.encode('utf-8')

            raise RuntimeError('skip rest of the method')

        mocker.patch(
            'pymqi.QueueManager.connect_with_options',
            mocked_connect_with_options,
        )
        pass
