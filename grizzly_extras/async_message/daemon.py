"""Daemon implementation that handles are requests.
Based on ZMQ PUB/SUB, where each request type is handled to a worker which is for a specific client.
"""
from __future__ import annotations

import logging
from concurrent import futures
from json import dumps as jsondumps
from json import loads as jsonloads
from signal import SIGINT, SIGTERM, Signals, signal
from threading import Event
from time import sleep
from typing import TYPE_CHECKING, Optional, Union, cast
from urllib.parse import urlparse
from uuid import uuid4

import setproctitle as proc
import zmq.green as zmq
from typing_extensions import Literal

from grizzly_extras.transformer import JsonBytesEncoder

from . import (
    LRU_READY,
    SPLITTER_FRAME,
    AsyncMessageHandler,
    AsyncMessageRequest,
    AsyncMessageResponse,
)

if TYPE_CHECKING:  # pragma: no cover
    from types import FrameType, TracebackType

abort: bool = False

def signal_handler(signum: Union[int, Signals], _frame: Optional[FrameType]) -> None:
    logger = logging.getLogger('signal_handler')
    logger.debug('received signal %r', signum)

    global abort  # noqa: PLW0603
    if not abort:
        logger.info('aborting due to %r', signum)
        abort = True


signal(SIGTERM, signal_handler)
signal(SIGINT, signal_handler)


class ThreadPoolExecutor(futures.ThreadPoolExecutor):
    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> Literal[False]:
        self.shutdown(wait=False)
        return False


class Worker:
    logger: logging.Logger
    identity: str
    context: zmq.Context

    integration: Optional[AsyncMessageHandler]

    _event: Event

    def __init__(self, context: zmq.Context, identity: str) -> None:
        self.logger = logging.getLogger(f'worker::{identity}')
        self.identity = identity
        self.context = context
        self.integration = None

        self._event = Event()

    def stop(self) -> None:
        self._event.set()

    def run(self) -> None:
        self.socket = self.context.socket(zmq.REQ)
        self.socket.setsockopt_string(zmq.IDENTITY, self.identity)
        self.socket.connect('inproc://workers')
        self.socket.send_string(LRU_READY)

        while not self._event.is_set():
            received = False
            try:
                request_proto = self.socket.recv_multipart(flags=zmq.NOBLOCK)
                received = True
            except zmq.Again:
                sleep(0.1)
                continue

            self.logger.debug("i'm alive! abort=%r, received=%r", abort, received)

            if not request_proto:
                self.logger.error('empty msg')
                continue

            request = cast(
                AsyncMessageRequest,
                jsonloads(request_proto[-1].decode()),
            )

            response: Optional[AsyncMessageResponse] = None

            try:
                if request['worker'] != self.identity:
                    message = f'got {request["worker"]}, expected {self.identity}'
                    raise RuntimeError(message)

                if self.integration is None:
                    integration_url = request.get('context', {}).get('url', None)
                    if integration_url is None:
                        message = 'no url found in request context'
                        raise RuntimeError(message)

                    parsed = urlparse(integration_url)

                    if parsed.scheme in ['mq', 'mqs']:
                        from .mq import AsyncMessageQueueHandler
                        self.integration = AsyncMessageQueueHandler(self.identity)
                    elif parsed.scheme == 'sb':
                        from .sb import AsyncServiceBusHandler
                        self.integration = AsyncServiceBusHandler(self.identity)
                    else:
                        message = f'integration for {parsed.scheme}:// is not implemented'
                        raise RuntimeError(message)
            except Exception as e:
                response = {
                    'worker': self.identity,
                    'response_time': 0,
                    'success': False,
                    'message': str(e),
                }

            if response is None and self.integration is not None:
                self.logger.debug('send request to handler')
                response = self.integration.handle(request)
                self.logger.debug('got response from handler')

            response_proto = [
                request_proto[0],
                SPLITTER_FRAME,
                jsondumps(response, cls=JsonBytesEncoder).encode(),
            ]

            self.socket.send_multipart(response_proto)

        if self.integration is not None:
            self.integration.close()

        self.socket.close()
        self.logger.info('stopped')


def create_router_socket(context: zmq.Context) -> zmq.Socket:
    socket = cast(zmq.Socket, context.socket(zmq.ROUTER))
    socket.setsockopt(zmq.LINGER, 0)
    socket.setsockopt(zmq.ROUTER_HANDOVER, 1)

    return socket


def router() -> None:  # noqa: C901, PLR0915
    logger = logging.getLogger('router')
    proc.setproctitle('grizzly-async-messaged')  # set appl name on ibm mq
    logger.debug('starting')

    context = zmq.Context()
    frontend = create_router_socket(context)
    backend = create_router_socket(context)

    frontend.bind('tcp://127.0.0.1:5554')
    backend.bind('inproc://workers')

    poller = zmq.Poller()
    poller.register(frontend, zmq.POLLIN)
    poller.register(backend, zmq.POLLIN)

    workers: dict[str, tuple[futures.Future, Worker]] = {}

    with ThreadPoolExecutor() as executor:
        def spawn_worker() -> None:
            identity = str(uuid4())

            worker = Worker(context, identity)

            future = executor.submit(worker.run)
            workers.update({identity: (future, worker)})
            logger.info('spawned worker %s', identity)

        workers_available: list[str] = []

        client_worker_map: dict[str, str] = {}

        spawn_worker()

        worker_id: str

        while not abort:
            socks = dict(poller.poll(timeout=1000))

            if not socks:
                continue

            logger.debug("i'm alive!")

            if socks.get(backend) == zmq.POLLIN:
                logger.debug('polling backend')
                try:
                    backend_response = backend.recv_multipart(flags=zmq.NOBLOCK)
                except zmq.Again:
                    sleep(0.1)
                    continue

                if not backend_response:
                    continue

                logger.debug('backend_response: %r', backend_response)
                reply = backend_response[2:]
                worker_id = backend_response[0].decode()

                if reply[0] != LRU_READY.encode():
                    logger.debug('sending %r', reply)
                    frontend.send_multipart(reply)
                    logger.debug('forwarding backend response from %s', worker_id)
                else:
                    logger.info('worker %s ready', worker_id)
                    workers_available.append(worker_id)

            if socks.get(frontend) == zmq.POLLIN:
                logger.debug('polling frontend')
                try:
                    msg = frontend.recv_multipart(flags=zmq.NOBLOCK)
                except zmq.Again:
                    sleep(0.1)
                    continue

                request_id = msg[0]
                payload = cast(AsyncMessageRequest, jsonloads(msg[-1].decode()))

                request_worker_id = payload.get('worker', None)
                request_client_id = payload.get('client', None)
                client_key: Optional[str] = None

                logger.debug('request_worker_id=%r (%r), request_client_id=%r (%r)', request_worker_id, type(request_worker_id), request_client_id, type(request_client_id))

                if request_client_id is not None:
                    integration_url = payload.get('context', {}).get('url', None)
                    parsed = urlparse(integration_url)
                    scheme = parsed.scheme
                    if isinstance(scheme, bytes):
                        scheme = scheme.decode()

                    client_key = f'{request_client_id}::{scheme}'

                if request_worker_id is None and client_key is not None:
                    request_worker_id = client_worker_map.get(client_key)

                if request_worker_id is None:
                    worker_id = workers_available.pop()

                    if client_key is not None:
                        client_worker_map.update({client_key: worker_id})

                    payload['worker'] = worker_id
                    logger.info('assigned worker %s to %s', worker_id, client_key)

                    if len(workers_available) == 0:
                        logger.debug('spawning an additional worker, for next client')
                        spawn_worker()
                else:
                    logger.debug('%s is assigned %s', request_client_id, request_worker_id)
                    worker_id = request_worker_id

                    if payload.get('worker', None) is None:
                        payload['worker'] = worker_id

                request = jsondumps(payload).encode()
                backend_request = [worker_id.encode(), SPLITTER_FRAME, request_id, SPLITTER_FRAME, request]
                backend.send_multipart(backend_request)

        logger.info('stopping')
        # for identity, (future, worker) in workers.items():
        for identity, (future, worker) in workers.items():
            if not future.running():
                continue

            worker.stop()

            # tell client that we aborted
            if worker.integration is not None:
                response = {
                    'success': False,
                    'worker': identity,
                    'message': 'abort',
                }

                response_proto = [
                    worker.identity.encode(),
                    SPLITTER_FRAME,
                    jsondumps(response, cls=JsonBytesEncoder).encode(),
                ]

                frontend.send_multipart(response_proto)

                worker.logger.debug('sent abort to client')


                worker.integration.close()
                worker.socket.close()
                worker.logger.info('socket closed')

                # stop worker
                cancelled = future.cancel()
                logger.info('worker %s cancelled: %r', identity, cancelled)
            else:
                # should complete when `worker.stop()` has had effect
                futures.wait([future])

        try:
            logger.debug('destroy zmq context')
            context.destroy(linger=0)
        except:
            logger.exception('failed to destroy zmq context')

    logger.info('stopped')

def main() -> int:
    try:
        router()
    except KeyboardInterrupt:
        return 1
    else:
        return 0
