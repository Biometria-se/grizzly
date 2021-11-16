from typing import Optional, Union, Tuple, Literal, Generator, Callable, Dict, cast
from time import monotonic as time, sleep
from contextlib import contextmanager
from json import dumps as jsondumps

from grizzly.exceptions import TransformerError
from grizzly.transformer import transformer
from grizzly.types import ResponseContentType, str_response_content_type

from . import AsyncMessageRequest, AsyncMessageResponse, AsyncMessageError, AsyncMessageHandler, JsonBytesEncoder, logger

#import pymqi
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

    @contextmanager
    def queue_browsing_context(self, endpoint: str) -> Generator[pymqi.Queue, None, None]:
        queue = pymqi.Queue(self.qmgr, endpoint,
            pymqi.CMQC.MQOO_FAIL_IF_QUIESCING
            | pymqi.CMQC.MQOO_INPUT_SHARED
            | pymqi.CMQC.MQOO_BROWSE)

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

    def _create_matching_gmo(self, msg_id: bytearray, message_wait: int) -> pymqi.GMO:
        gmo = self._create_gmo(message_wait)
        gmo['MatchOptions'] = pymqi.CMQC.MQMO_MATCH_MSG_ID
        gmo['MsgId'] = msg_id
        return gmo

    def _create_browsing_gmo(self) -> pymqi.GMO:
        return pymqi.GMO(
            Options=pymqi.CMQC.MQGMO_BROWSE_FIRST,
        )

    def _find_message(self, queue_name: str, predicate: str, content_type: ResponseContentType, message_wait: int) -> Optional[bytearray]:
        start_time = time()

        transform = transformer.available.get(content_type, None)
        if transform is None:
            raise AsyncMessageError(f'{self.__class__.__name__}: could not find a transformer for {content_type.name}')

        try:
            get_values = transform.parser(predicate)
        except TransformerError as e:
            raise AsyncMessageError(f'{self.__class__.__name__}: {str(e.message)}')

        with self.queue_browsing_context(endpoint=queue_name) as browse_queue:
            # Check the queue over and over again until timeout, if nothing was found
            while True:
                gmo = self._create_browsing_gmo()
                md = pymqi.MD()

                try:
                    # Check all current messages
                    while True:
                        message = browse_queue.get(None, md, gmo)
                        payload = message.decode()

                        try:
                            _, payload = transform.transform(content_type, payload)
                        except TransformerError as e:
                            raise AsyncMessageError(f'{self.__class__.__name__}: {str(e.message)}')

                        values = get_values(payload)

                        if len(values) > 0:
                            # Found a matching message, return message id
                            return cast(bytearray, md['MsgId'])

                        gmo.Options = pymqi.CMQC.MQGMO_BROWSE_NEXT

                except pymqi.MQMIError as e:
                    if e.comp == pymqi.CMQC.MQCC_FAILED and e.reason == pymqi.CMQC.MQRC_NO_MSG_AVAILABLE:
                        # No messages, that's OK
                        pass
                    else:
                        # Some other error condition.
                        raise AsyncMessageError(f'{self.__class__.__name__}: {str(e)}')

                # Check elapsed time, sleep and check again if we haven't timed out
                cur_time = time()
                if cur_time - start_time >= message_wait:
                    raise AsyncMessageError(f'{self.__class__.__name__}: timeout while waiting for matching message')
                else:
                    sleep(0.5)

    def _get_content_type(self, request: AsyncMessageRequest) -> ResponseContentType:
        content_type: ResponseContentType = ResponseContentType.GUESS
        value: Optional[str] = cast(Optional[str], request.get('context', {}).get('content_type', None))
        if value:
            content_type = str_response_content_type(value)
        return content_type

    def _request(self, request: AsyncMessageRequest) -> AsyncMessageResponse:
        if self.qmgr is None:
            raise AsyncMessageError('not connected')

        queue_name = request.get('context', {}).get('queue', None)
        if queue_name is None:
            raise AsyncMessageError('no queue specified')

        predicate = request.get('context', {}).get('predicate', None)

        action = request['action']

        md = pymqi.MD()

        message_wait: int = request.get('context', {}).get('message_wait', None) or self.message_wait_global

        msg_id_to_fetch: Optional[bytearray] = None
        if action == 'GET' and predicate is not None:
            content_type = self._get_content_type(request)
            start_time = time()
            # Browse for any matching message
            msg_id_to_fetch = self._find_message(queue_name, predicate, content_type, message_wait)
            elapsed_time = int(time() - start_time)
            # Adjust message_wait for getting the message
            message_wait -= elapsed_time

        with self.queue_context(endpoint=queue_name) as queue:
            if action == 'PUT':
                payload = request.get('payload', None)
                response_length = len(payload) if payload is not None else 0
                queue.put(payload, md)
            elif action == 'GET':
                gmo: Optional[pymqi.GMO] = None

                if msg_id_to_fetch:
                    gmo = self._create_matching_gmo(msg_id_to_fetch, message_wait)
                else:
                    gmo = self._create_gmo(message_wait)

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
