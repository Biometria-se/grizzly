from typing import Any, Optional, Generator, Dict, cast
from time import perf_counter as time, sleep
from contextlib import contextmanager

from grizzly_extras.transformer import transformer, TransformerError, TransformerContentType
from grizzly_extras.arguments import parse_arguments, get_unsupported_arguments

from grizzly_extras.async_message.utils import tohex


from grizzly_extras.async_message import (
    AsyncMessageRequest,
    AsyncMessageResponse,
    AsyncMessageError,
    AsyncMessageRequestHandler,
    AsyncMessageHandler,
    register
)

try:
    import pymqi
except:
    from grizzly_extras import dummy_pymqi as pymqi

from .rfh2 import Rfh2Decoder, Rfh2Encoder

__all__ = [
    'AsyncMessageQueueHandler',
    'Rfh2Decoder',
    'Rfh2Encoder',
]

handlers: Dict[str, AsyncMessageRequestHandler] = {}


class AsyncMessageQueueHandler(AsyncMessageHandler):
    qmgr: Optional[pymqi.QueueManager] = None

    def __init__(self, worker: str) -> None:
        if pymqi.__name__ == 'grizzly_extras.dummy_pymqi':
            pymqi.raise_for_error(self.__class__)

        super().__init__(worker)
        self.header_type: Optional[str] = None

    def close(self) -> None:
        if self.qmgr is not None:
            self.logger.debug('closing queue manager connection')
            self.qmgr.disconnect()
            self.qmgr = None

    @contextmanager
    def queue_context(self, endpoint: str, browsing: Optional[bool] = False) -> Generator[pymqi.Queue, None, None]:
        queue: Optional[pymqi.Queue] = None
        if browsing:
            queue = pymqi.Queue(
                self.qmgr, endpoint,
                pymqi.CMQC.MQOO_FAIL_IF_QUIESCING
                | pymqi.CMQC.MQOO_INPUT_SHARED
                | pymqi.CMQC.MQOO_BROWSE
            )
        else:
            queue = pymqi.Queue(self.qmgr, endpoint)

        try:
            yield queue
        finally:
            queue.close()

    @register(handlers, 'DISC')
    def disconnect(self, request: AsyncMessageRequest) -> AsyncMessageResponse:
        self.close()
        self.qmgr = None

        return {
            'message': 'disconnected',
        }

    @register(handlers, 'CONN')
    def connect(self, request: AsyncMessageRequest) -> AsyncMessageResponse:
        context = request.get('context', None)
        if context is None:
            raise AsyncMessageError('no context in request')

        if self.qmgr is not None:
            return {
                'message': 're-used connection',
            }

        connection = context['connection']
        queue_manager = context['queue_manager']
        channel = context['channel']
        username = context.get('username', None)
        password = context.get('password', None)
        key_file = context.get('key_file', None)
        cert_label = context.get('cert_label', None) or username
        ssl_cipher = context.get('ssl_cipher', None) or 'ECDHE_RSA_AES_256_GCM_SHA384'
        heartbeat_interval = context.get('heartbeat_interval', None) or 300
        connect_opts = pymqi.CMQC.MQCNO_RECONNECT

        cd = pymqi.CD(
            ChannelName=channel.encode(),
            ConnectionName=connection.encode(),
            ChannelType=pymqi.CMQC.MQCHT_CLNTCONN,
            TransportType=pymqi.CMQC.MQXPT_TCP,
            HeartbeatInterval=heartbeat_interval,
        )
        self.qmgr = pymqi.QueueManager(None)

        if key_file is not None:
            cd['SSLCipherSpec'] = ssl_cipher.encode() if ssl_cipher is not None else None

            sco = pymqi.SCO(
                KeyRepository=key_file.encode(),
                CertificateLabel=cert_label.encode() if cert_label is not None else None,
            )
        else:
            sco = pymqi.SCO()

        self.qmgr.connect_with_options(
            queue_manager,
            user=username.encode() if username is not None else None,
            password=password.encode() if password is not None else None,
            cd=cd,
            sco=sco,
            opts=connect_opts,
        )

        self.message_wait = context.get('message_wait', None) or 0
        self.header_type = context.get('header_type', None)

        self.logger.info(f'connected to {connection}')

        return {
            'message': 'connected',
        }

    def _create_gmo(self, message_wait: Optional[int] = None, browsing: Optional[bool] = False) -> pymqi.GMO:
        gmo: Optional[pymqi.GMO] = None
        if message_wait is not None and message_wait > 0:
            gmo = pymqi.GMO(
                Options=pymqi.CMQC.MQGMO_WAIT | pymqi.CMQC.MQGMO_FAIL_IF_QUIESCING,
                WaitInterval=message_wait * 1000,
            )
        else:
            gmo = pymqi.GMO()

        if browsing:
            gmo.Options |= pymqi.CMQC.MQGMO_BROWSE_FIRST
        else:
            gmo.Options |= pymqi.CMQC.MQGMO_SYNCPOINT

        return gmo

    def _get_payload(self, message: bytes) -> str:
        if Rfh2Decoder.is_rfh2(message):
            rfh2_decoder = Rfh2Decoder(message)
            return rfh2_decoder.get_payload().decode()
        else:
            return message.decode()

    def _create_md(self) -> pymqi.MD:
        if self.header_type and self.header_type == 'rfh2':
            return Rfh2Encoder.create_md()
        else:
            return pymqi.MD()

    def _find_message(self, queue_name: str, expression: str, content_type: TransformerContentType, message_wait: Optional[int]) -> Optional[bytearray]:
        start_time = time()

        self.logger.debug(f'_find_message: searching {queue_name} for messages matching: {expression}, content_type {content_type.name.lower()}')
        transform = transformer.available.get(content_type, None)
        if transform is None:
            raise AsyncMessageError(f'could not find a transformer for {content_type.name}')

        try:
            get_values = transform.parser(expression)
        except Exception as e:
            raise AsyncMessageError(str(e))

        retries = 1
        with self.queue_context(endpoint=queue_name, browsing=True) as browse_queue:
            # Check the queue over and over again until timeout, if nothing was found
            while True:
                gmo = self._create_gmo(browsing=True)

                try:
                    # Check all current messages
                    while True:
                        md = self._create_md()
                        message = browse_queue.get(None, md, gmo)
                        payload = self._get_payload(message)

                        try:
                            payload = transform.transform(payload)
                        except TransformerError as e:
                            raise AsyncMessageError(e.message)

                        values = get_values(payload)

                        if len(values) > 0:
                            # Found a matching message, return message id
                            self.logger.debug(f'_find_message: found matching message: {md["MsgId"]} after {retries} tries')
                            return cast(bytearray, md['MsgId'])

                        gmo.Options = pymqi.CMQC.MQGMO_BROWSE_NEXT

                except pymqi.MQMIError as e:
                    if e.comp == pymqi.CMQC.MQCC_FAILED and e.reason == pymqi.CMQC.MQRC_NO_MSG_AVAILABLE:
                        # No messages, that's OK
                        pass
                    elif e.reason == pymqi.CMQC.MQRC_TRUNCATED_MSG_FAILED:
                        self.logger.warning('got MQRC_TRUNCATED_MSG_FAILED while browsing messages')
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
                    retries += 1
                    sleep(0.5)

    def _get_content_type(self, request: AsyncMessageRequest) -> TransformerContentType:
        content_type: TransformerContentType = TransformerContentType.UNDEFINED
        value: Optional[str] = request.get('context', {}).get('content_type', None)
        if value:
            content_type = TransformerContentType.from_string(value)
        return content_type

    def _get_safe_message_descriptor(self, md: pymqi.MD) -> Dict[str, Any]:
        metadata: Dict[str, Any] = md.get()

        if 'MsgId' in metadata:
            metadata['MsgId'] = tohex(metadata['MsgId'])

        return metadata

    def _request(self, request: AsyncMessageRequest) -> AsyncMessageResponse:
        if self.qmgr is None:
            raise AsyncMessageError('not connected')

        endpoint = request.get('context', {}).get('endpoint', None)
        if endpoint is None:
            raise AsyncMessageError('no endpoint specified')

        try:
            arguments = parse_arguments(endpoint, separator=':')
            unsupported_arguments = get_unsupported_arguments(['queue', 'expression', 'max_message_size'], arguments)
            if len(unsupported_arguments) > 0:
                raise ValueError(f'arguments {", ".join(unsupported_arguments)} is not supported')
        except ValueError as e:
            raise AsyncMessageError(str(e))

        queue_name = arguments.get('queue', None)
        expression = arguments.get('expression', None)
        max_message_size: Optional[int] = int(arguments.get('max_message_size', '0'))

        if not max_message_size:
            max_message_size = None

        action = request['action']

        if action != 'GET' and expression is not None:
            raise AsyncMessageError(f'argument expression is not allowed for action {action}')

        message_wait = request.get('context', {}).get('message_wait', None) or self.message_wait
        metadata = request.get('context', {}).get('metadata', None)

        retries: int = 0
        while retries < 5:
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
                    message_wait = max(message_wait - elapsed_time, 0)
                    self.logger.debug(f'_request: remaining message_wait after finding message: {message_wait}')

            md = self._create_md()
            with self.queue_context(endpoint=queue_name) as queue:
                do_retry: bool = False

                self.logger.info(f'executing {action} on {queue_name}')
                start = time()

                try:
                    if action == 'PUT':
                        request_payload = payload = request.get('payload', None)
                        if self.header_type:
                            if self.header_type == 'rfh2':
                                rfh2_encoder = Rfh2Encoder(payload=cast(str, payload).encode(), queue_name=queue_name, metadata=metadata)
                                request_payload = rfh2_encoder.get_message()
                            else:
                                raise AsyncMessageError(f'Invalid header_type: {self.header_type}')

                        response_length = len(request_payload) if request_payload is not None else 0
                        queue.put(request_payload, md)

                    elif action == 'GET':
                        payload = None

                        if msg_id_to_fetch is not None:
                            gmo = self._create_gmo()
                            gmo.MatchOptions = pymqi.CMQC.MQMO_MATCH_MSG_ID
                            md.MsgId = msg_id_to_fetch
                        else:
                            gmo = self._create_gmo(message_wait)

                        try:
                            try:
                                message = queue.get(max_message_size, md, gmo)
                                payload = self._get_payload(message)
                                response_length = len((payload or '').encode())

                                if response_length == 0:
                                    do_retry = True  # we should consume the empty message, not put it back on queue
                                    self.logger.warning('message with size 0 bytes consumed, get next message')
                                elif retries > 0:
                                    self.logger.warning(f'got message after {retries} retries')

                                self.qmgr.commit()
                            except:
                                self.qmgr.backout()
                                raise
                        except pymqi.MQMIError as e:
                            if msg_id_to_fetch is not None and e.comp == pymqi.CMQC.MQCC_FAILED and e.reason == pymqi.CMQC.MQRC_NO_MSG_AVAILABLE:
                                # Message disappeared, retry
                                do_retry = True
                            elif e.reason == pymqi.CMQC.MQRC_TRUNCATED_MSG_FAILED:
                                original_length = getattr(e, 'original_length', None)
                                self.logger.warning(f'got MQRC_TRUNCATED_MSG_FAILED while getting message, {retries=}, {original_length=}')
                                if max_message_size is None:
                                    # Concurrency issue, retry
                                    do_retry = True
                                else:
                                    raise AsyncMessageError(f'message with size {original_length} bytes does not fit in message buffer of {max_message_size} bytes')
                            elif e.reason == pymqi.CMQC.MQRC_BACKED_OUT:
                                warning_message = ['got MQRC_BACKED_OUT while getting message', f'{retries=}']
                                self.logger.warning(', '.join(warning_message))
                                do_retry = True
                            else:
                                # Some other error condition.
                                self.logger.error(str(e), exc_info=True)
                                raise AsyncMessageError(str(e))
                except pymqi.PYIFError as e:
                    if e.error.strip() == 'not open':
                        self.logger.warning('reconnecting to queue manager')
                        self.disconnect({})
                        self.connect(request)
                        do_retry = True
                    else:
                        self.logger.error(str(e), exc_info=True)
                        raise AsyncMessageError(str(e))

                if do_retry:
                    retries += 1
                    sleep(retries * retries * 0.5)
                else:
                    delta = (time() - start) * 1000
                    self.logger.info(f'{action} on {queue_name} took {delta} ms, {response_length=}, {retries=}')
                    return {
                        'payload': payload,
                        'metadata': self._get_safe_message_descriptor(md),
                        'response_length': response_length,
                    }

        raise AsyncMessageError(f'failed after {retries} retries')

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
