from typing import Optional, Generator, Dict, cast
from time import monotonic as time, sleep
from contextlib import contextmanager

from ..transformer import transformer, TransformerError, TransformerContentType
from ..arguments import parse_arguments, get_unsupported_arguments


from . import (
    AsyncMessageRequest,
    AsyncMessageResponse,
    AsyncMessageError,
    AsyncMessageRequestHandler,
    AsyncMessageHandler,
    register,
    logger,
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
    def queue_context(self, endpoint: str, browsing: Optional[bool] = False) -> Generator[pymqi.Queue, None, None]:
        queue: Optional[pymqi.Queue] = None
        if browsing:
            queue = pymqi.Queue(self.qmgr, endpoint,
                pymqi.CMQC.MQOO_FAIL_IF_QUIESCING
                | pymqi.CMQC.MQOO_INPUT_SHARED
                | pymqi.CMQC.MQOO_BROWSE)
        else:
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

    def _create_gmo(self, message_wait: Optional[int] = None, browsing: Optional[bool] = False) -> pymqi.GMO:
        gmo: Optional[pymqi.GMO] = None
        if message_wait is not None and message_wait > 0:
            gmo = pymqi.GMO(
                Options=pymqi.CMQC.MQGMO_WAIT | pymqi.CMQC.MQGMO_FAIL_IF_QUIESCING,
                WaitInterval=message_wait*1000,
            )
        else:
            gmo = pymqi.GMO()

        if browsing:
            gmo.Options |= pymqi.CMQC.MQGMO_BROWSE_FIRST

        return gmo

    def _find_message(self, queue_name: str, expression: str, content_type: TransformerContentType, message_wait: Optional[int]) -> Optional[bytearray]:
        start_time = time()

        logger.debug(f'{self.worker}: _find_message: searching {queue_name} for messages matching: {expression}, content_type {content_type.name.lower()}')
        transform = transformer.available.get(content_type, None)
        if transform is None:
            raise AsyncMessageError(f'could not find a transformer for {content_type.name}')

        try:
            get_values = transform.parser(expression)
        except Exception as e:
            raise AsyncMessageError(str(e))

        with self.queue_context(endpoint=queue_name, browsing=True) as browse_queue:
            # Check the queue over and over again until timeout, if nothing was found
            while True:
                gmo = self._create_gmo(browsing=True)

                try:
                    # Check all current messages
                    while True:
                        md = pymqi.MD()
                        message = browse_queue.get(None, md, gmo)
                        payload = message.decode()

                        try:
                            _, payload = transform.transform(content_type, payload)
                        except TransformerError as e:
                            raise AsyncMessageError(e.message)

                        values = get_values(payload)

                        if len(values) > 0:
                            # Found a matching message, return message id
                            logger.debug(f'{self.worker}: _find_message: found matching message: {md["MsgId"]}')
                            return cast(bytearray, md['MsgId'])

                        gmo.Options = pymqi.CMQC.MQGMO_BROWSE_NEXT

                except pymqi.MQMIError as e:
                    if e.comp == pymqi.CMQC.MQCC_FAILED and e.reason == pymqi.CMQC.MQRC_NO_MSG_AVAILABLE:
                        # No messages, that's OK
                        pass
                    else:
                        # Some other error condition.
                        raise AsyncMessageError(str(e))

                # Check elapsed time, sleep and check again if we haven't timed out
                cur_time = time()
                if message_wait is not None and cur_time - start_time >= message_wait:
                    raise AsyncMessageError('timeout while waiting for matching message')
                elif message_wait is None:
                    return None
                else:
                    logger.debug(f'{self.worker}: _find_message: no matching message found, trying again after some sleep')
                    sleep(0.5)

    def _get_content_type(self, request: AsyncMessageRequest) -> TransformerContentType:
        content_type: TransformerContentType = TransformerContentType.GUESS
        value: Optional[str] = request.get('context', {}).get('content_type', None)
        if value:
            content_type = TransformerContentType.from_string(value)
        return content_type

    def _request(self, request: AsyncMessageRequest) -> AsyncMessageResponse:
        if self.qmgr is None:
            raise AsyncMessageError('not connected')

        endpoint = request.get('context', {}).get('endpoint', None)
        if endpoint is None:
            raise AsyncMessageError('no endpoint specified')

        try:
            arguments = parse_arguments(endpoint, separator=':')
            unsupported_arguments = get_unsupported_arguments(['queue', 'expression'], arguments)
            if len(unsupported_arguments) > 0:
                raise ValueError(f'arguments {", ".join(unsupported_arguments)} is not supported')
        except ValueError as e:
            raise AsyncMessageError(str(e))

        queue_name = arguments.get('queue', None)
        expression = arguments.get('expression', None)

        action = request['action']

        if action != 'GET' and expression is not None:
            raise AsyncMessageError(f'argument expression is not allowed for action {action}')

        message_wait = request.get('context', {}).get('message_wait', None) or self.message_wait

        msg_id_to_fetch: Optional[bytearray] = None
        if action == 'GET' and expression is not None:
            content_type = self._get_content_type(request)
            start_time = time()
            # Browse for any matching message
            msg_id_to_fetch = self._find_message(queue_name, expression, content_type, message_wait)
            if msg_id_to_fetch is None:
                raise AsyncMessageError('no matching message found')

            elapsed_time = int(time() - start_time)
            # Adjust message_wait for getting the message
            if message_wait is not None:
                message_wait -= elapsed_time
                logger.debug(f'{self.worker}: _request: remaining message_wait after finding message: {message_wait}')

        md = pymqi.MD()
        with self.queue_context(endpoint=queue_name) as queue:
            if action == 'PUT':
                payload = request.get('payload', None)
                response_length = len(payload) if payload is not None else 0
                queue.put(payload, md)
            elif action == 'GET':
                gmo = self._create_gmo(message_wait)

                if msg_id_to_fetch is not None:
                    gmo.MatchOptions = pymqi.CMQC.MQMO_MATCH_MSG_ID
                    md.MsgId = msg_id_to_fetch

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
