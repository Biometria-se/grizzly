from typing import Optional, Union, Tuple, Literal, Generator, Callable, Dict
from time import monotonic as time
from contextlib import contextmanager
from json import dumps as jsondumps

from . import AsyncMessageRequest, AsyncMessageResponse, AsyncMessageError, AsyncMessageHandler, JsonBytesEncoder, logger

try:
    import pymqi
except:
    from grizzly_extras import dummy_pymqi as pymqi


AsyncMessageQueueGetArguments = Union[Tuple[Literal[None], pymqi.MD], Tuple[Literal[None], pymqi.MD, pymqi.GMO]]

AsyncMessageRequestHandler = Callable[['AsyncMessageQueue', AsyncMessageRequest], AsyncMessageResponse]

handlers: Dict[str, AsyncMessageRequestHandler] = {}


def register(action: str, *actions: str) -> Callable[[AsyncMessageRequestHandler], None]:
    def decorator(func: AsyncMessageRequestHandler) -> None:
        for a in (action, *actions):
            if a in handlers:
                continue

            handlers.update({a: func})

    return decorator


class AsyncMessageQueue(AsyncMessageHandler):
    qmgr: Optional[pymqi.QueueManager] = None
    message_wait_global: int = 0

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

    @register('CONN')
    def connect(self, request: AsyncMessageRequest) -> AsyncMessageResponse:
        if self.qmgr is not None:
            raise AsyncMessageError('already connected')

        context = request.get('context', None)
        if context is None:
            raise AsyncMessageError('no context')

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

        self.message_wait_global = context.get('message_wait', None) or 0

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

        queue_name = request.get('context', {}).get('queue', None)
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
                elif self.message_wait_global > 0:
                    gmo = self._create_gmo(self.message_wait_global)

                payload = queue.get(None, md, gmo).decode()
                response_length = len(payload) if payload is not None else 0

            return {
                'payload': payload,
                'metadata': md.get(),
                'response_length': response_length,
            }

    @register('PUT', 'SEND')
    def put(self, request: AsyncMessageRequest) -> AsyncMessageResponse:
        request['action'] = 'PUT'

        if request.get('payload', None) is None:
            raise AsyncMessageError('no payload')

        return self._request(request)

    @register('GET', 'RECEIVE')
    def get(self, request: AsyncMessageRequest) -> AsyncMessageResponse:
        request['action'] = 'GET'

        if request.get('payload', None) is not None:
            raise AsyncMessageError('payload not allowed')

        return self._request(request)

    def handler(self, request: AsyncMessageRequest) -> AsyncMessageResponse:
        action = request['action']
        action_handler = handlers.get(action, None)

        logger.debug(f'handling {action}')
        logger.debug(jsondumps(request, indent=2, cls=JsonBytesEncoder))

        response: AsyncMessageResponse

        start_time = time()

        try:
            if action_handler is None:
                raise AsyncMessageError(f'no implementation for {action}')

            response = action_handler(self, request)
            response['success'] = True
        except Exception as e:
            response = {
                'success': False,
                'message': f'{action}: {e.__class__.__name__}="{str(e)}"',
            }
        finally:
            total_time = int((time() - start_time) * 1000)
            response.update({
                'worker': self.worker,
                'response_time': total_time,
            })

            logger.debug(f'handled {action}')
            logger.debug(jsondumps(response, indent=2, cls=JsonBytesEncoder))

            return response
