import logging
import json

from typing import Optional, Dict, Any, TypedDict, Tuple, Callable, Generator, Literal, Union, cast
from time import monotonic as time
from contextlib import contextmanager
from json import JSONEncoder

try:
    import pymqi
except:
    from grizzly_extras import dummy_pymqi as pymqi


MessageQueueMetadata = Optional[Dict[str, Any]]
MessageQueuePayload = Optional[Any]


class JsonBytesEncoder(JSONEncoder):
    def default(self, o: Any) -> Any:
        if isinstance(o, bytes):
            try:
                return o.decode('utf-8')
            except:
                return o.decode('latin-1')

        return JSONEncoder.default(self, o)


class MessageQueueContext(TypedDict, total=False):
    queue_manager: str
    connection: str
    channel: str
    username: Optional[str]
    password: Optional[str]
    key_file: Optional[str]
    cert_label: Optional[str]
    ssl_cipher: Optional[str]
    message_wait: Optional[int]
    queue: str


class MessageQueueRequest(TypedDict, total=False):
    action: str
    worker: Optional[str]
    context: MessageQueueContext
    payload: MessageQueuePayload


class MessageQueueResponse(TypedDict, total=False):
    success: bool
    worker: str
    message: Optional[str]
    payload: MessageQueuePayload
    metadata: MessageQueueMetadata
    response_length: int
    response_time: int


class MessageQueueError(Exception):
    pass


LRU_READY = '\x01'
SPLITTER_FRAME = ''.encode()

logger = logging.getLogger(__name__)
logging.basicConfig(format="[%(levelname)-5s] %(name)s: %(message)s", level=logging.INFO)

MessageQueueRequestHandler = Callable[['MessageQueue', MessageQueueRequest], MessageQueueResponse]

handlers: Dict[str, MessageQueueRequestHandler] = {}

MessageQueueGetArguments = Union[Tuple[Literal[None], pymqi.MD], Tuple[Literal[None], pymqi.MD, pymqi.GMO]]


def register(action: str, *actions: str) -> Callable[[MessageQueueRequestHandler], None]:
    def decorator(func: MessageQueueRequestHandler) -> None:
        for a in (action, *actions):
            if a in handlers:
                continue

            handlers.update({a: func})

    return decorator


class MessageQueue:
    qmgr: Optional[pymqi.QueueManager] = None
    worker: str
    message_wait_global: int = 0

    def __init__(self, worker: str) -> None:
        if pymqi.__name__ == 'grizzly_extras.dummy_pymqi':
            raise NotImplementedError('MessageQueue could not import pymqi, have you installed IBM MQ dependencies?')

        self.worker = worker


    @contextmanager
    def queue_context(self, endpoint: str) -> Generator[pymqi.Queue, None, None]:
        queue = pymqi.Queue(self.qmgr, endpoint)

        try:
            yield queue
        finally:
            queue.close()

    @register('CONN')
    def connect(self, request: MessageQueueRequest) -> MessageQueueResponse:
        if self.qmgr is not None:
            raise MessageQueueError('already connected')

        context = request.get('context', None)
        if context is None:
            raise MessageQueueError('no context')

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

    def _request(self, request: MessageQueueRequest) -> MessageQueueResponse:
        if self.qmgr is None:
            raise MessageQueueError('not connected')

        queue_name = request.get('context', {}).get('queue', None)
        if queue_name is None:
            raise MessageQueueError('no queue specified')

        action = request['action']

        md = pymqi.MD()

        with self.queue_context(queue_name) as queue:
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
    def put(self, request: MessageQueueRequest) -> MessageQueueResponse:
        request['action'] = 'PUT'

        if request.get('payload', None) is None:
            raise MessageQueueError('no payload')

        return self._request(request)

    @register('GET', 'RECEIVE')
    def get(self, request: MessageQueueRequest) -> MessageQueueResponse:
        request['action'] = 'GET'

        if request.get('payload', None) is not None:
            raise MessageQueueError('payload not allowed')

        return self._request(request)

    def handler(self, request: MessageQueueRequest) -> MessageQueueResponse:
        action = request['action']
        action_handler = handlers.get(action, None)

        logger.debug(f'handling {action}')
        logger.debug(json.dumps(request, indent=2, cls=JsonBytesEncoder))

        response: MessageQueueResponse

        start_time = time()

        try:
            if action_handler is None:
                raise MessageQueueError(f'no implementation for {action}')

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
            logger.debug(json.dumps(response, indent=2, cls=JsonBytesEncoder))

            return response
