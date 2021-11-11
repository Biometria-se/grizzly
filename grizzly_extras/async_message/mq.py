from typing import Optional, Generator, Dict
from contextlib import contextmanager

from . import (
    AsyncMessageRequest,
    AsyncMessageResponse,
    AsyncMessageError,
    AsyncMessageRequestHandler,
    AsyncMessageHandler,
    register,
)

try:
    import pymqi
except:
    from grizzly_extras import dummy_pymqi as pymqi


__all__ = [
    'AsyncMessageQueueHandler',
]


handlers: Dict[str, AsyncMessageRequestHandler] = {}


class AsyncMessageQueueHandler(AsyncMessageHandler):
    qmgr: Optional[pymqi.QueueManager] = None

    def __init__(self, worker: str) -> None:
        if pymqi.__name__ == 'grizzly_extras.dummy_pymqi':
            raise NotImplementedError(f'{self.__class__.__name__} could not import pymqi, have you installed IBM MQ dependencies?')

        super().__init__(worker)

    @contextmanager
    def queue_context(self, endpoint: str) -> Generator[pymqi.Queue, None, None]:
        queue = pymqi.Queue(self.qmgr, endpoint)

        try:
            yield queue
        finally:
            queue.close()

    @register(handlers, 'CONN')
    def connect(self, request: AsyncMessageRequest) -> AsyncMessageResponse:
        if self.qmgr is not None:
            raise AsyncMessageError('already connected')

        context = request.get('context', None)
        if context is None:
            raise AsyncMessageError('no context in request')

        connection = context['connection']
        queue_manager = context['queue_manager']
        channel = context['channel']
        username = context.get('username', None)
        password = context.get('password', None)
        key_file = context.get('key_file', None)
        cert_label = context.get('cert_label', None) or username
        ssl_cipher = context.get('ssl_cipher', None) or 'ECDHE_RSA_AES_256_GCM_SHA384'

        if key_file is not None:
            cd = pymqi.CD(
                ChannelName=channel.encode(),
                ConnectionName=connection.encode(),
                ChannelType=pymqi.CMQC.MQCHT_CLNTCONN,
                TransportType=pymqi.CMQC.MQXPT_TCP,
                SSLCipherSpec=ssl_cipher.encode() if ssl_cipher is not None else None,
            )

            sco = pymqi.SCO(
                KeyRepository=key_file.encode(),
                CertificateLabel=cert_label.encode() if cert_label is not None else None,
            )

            self.qmgr = pymqi.QueueManager(None)
            self.qmgr.connect_with_options(
                queue_manager,
                user=username.encode() if username is not None else None,
                password=password.encode() if password is not None else None,
                cd=cd,
                sco=sco,
            )
        else:
            self.qmgr = pymqi.connect(
                queue_manager,
                channel,
                connection,
                username,
                password,
            )

        self.message_wait = context.get('message_wait', None) or 0

        return {
            'message': 'connected',
        }

    def _create_gmo(self, message_wait: int) -> pymqi.GMO:
        return pymqi.GMO(
            Options=pymqi.CMQC.MQGMO_WAIT | pymqi.CMQC.MQGMO_FAIL_IF_QUIESCING,
            WaitInterval=message_wait*1000,
        )

    def _request(self, request: AsyncMessageRequest) -> AsyncMessageResponse:
        if self.qmgr is None:
            raise AsyncMessageError('not connected')

        queue_name = request.get('context', {}).get('endpoint', None)
        if queue_name is None:
            raise AsyncMessageError('no queue specified')

        action = request['action']

        md = pymqi.MD()

        with self.queue_context(endpoint=queue_name) as queue:
            if action == 'PUT':
                payload = request.get('payload', None)
                response_length = len(payload) if payload is not None else 0
                queue.put(payload, md)
            elif action == 'GET':
                message_wait: int = request.get('context', {}).get('message_wait', None) or 0

                gmo: Optional[pymqi.GMO] = None

                if message_wait > 0:
                    gmo = self._create_gmo(message_wait)
                elif self.message_wait is not None and self.message_wait > 0:
                    gmo = self._create_gmo(self.message_wait)

                payload = queue.get(None, md, gmo).decode()
                response_length = len(payload) if payload is not None else 0

            return {
                'payload': payload,
                'metadata': md.get(),
                'response_length': response_length,
            }

    @register(handlers, 'PUT', 'SEND')
    def put(self, request: AsyncMessageRequest) -> AsyncMessageResponse:
        request['action'] = 'PUT'

        if request.get('payload', None) is None:
            raise AsyncMessageError('no payload')

        return self._request(request)

    @register(handlers, 'GET', 'RECEIVE')
    def get(self, request: AsyncMessageRequest) -> AsyncMessageResponse:
        request['action'] = 'GET'

        if request.get('payload', None) is not None:
            raise AsyncMessageError('payload not allowed')

        return self._request(request)

    def get_handler(self, action: str) -> Optional[AsyncMessageRequestHandler]:
        return handlers.get(action, None)
