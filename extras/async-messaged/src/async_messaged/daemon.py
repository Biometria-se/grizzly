"""Daemon implementation that handles are requests.
Based on ZMQ PUB/SUB, where each request type is handled to a worker which is for a specific client.
"""

from __future__ import annotations

import logging
from concurrent import futures
from contextlib import suppress
from json import dumps as jsondumps
from json import loads as jsonloads
from multiprocessing import Process
from signal import SIGINT, SIGTERM, Signals, signal
from threading import Event
from time import sleep
from typing import TYPE_CHECKING, Literal, cast
from urllib.parse import urlparse
from uuid import uuid4

import setproctitle as proc
import zmq.green as zmq
from grizzly_common.transformer import JsonBytesEncoder
from zmq import sugar as ztypes

from . import (
    LRU_READY,
    SPLITTER_FRAME,
    AsyncMessageHandler,
    AsyncMessageRequest,
    AsyncMessageResponse,
)

if TYPE_CHECKING:  # pragma: no cover
    from types import FrameType, TracebackType


class ThreadPoolExecutor(futures.ThreadPoolExecutor):
    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> Literal[False]:
        self.shutdown(wait=False, cancel_futures=True)
        return False


class Worker:
    logger: logging.Logger
    identity: str
    context: ztypes.Context

    integration: AsyncMessageHandler | None

    _event: Event

    def __init__(self, context: ztypes.Context, identity: str, event: Event | None = None) -> None:
        self.logger = logging.getLogger(f'worker::{identity}')
        self.identity = identity
        self.context = context
        self.integration = None
        self._event = Event() if event is None else event
        self.socket = self.context.socket(zmq.REQ)
        self.socket.setsockopt_string(zmq.IDENTITY, self.identity)
        self.socket.connect('inproc://workers')
        self.socket.send_string(LRU_READY)

    def _create_integration(self, request: AsyncMessageRequest) -> AsyncMessageHandler:
        integration_url = request.get('context', {}).get('url', None)
        if integration_url is None:
            message = 'no url found in request context'
            raise RuntimeError(message)

        parsed = urlparse(integration_url)

        if parsed.scheme in ['mq', 'mqs']:
            from .mq import AsyncMessageQueueHandler  # noqa: PLC0415

            return AsyncMessageQueueHandler(self.identity, event=self._event)

        if parsed.scheme == 'sb':
            from .sb import AsyncServiceBusHandler  # noqa: PLC0415

            return AsyncServiceBusHandler(self.identity, event=self._event)

        message = f'integration for {parsed.scheme}:// is not implemented'
        raise RuntimeError(message)

    def run(self) -> None:
        connected = True

        try:
            while not self._event.is_set() and connected:
                received = False
                try:
                    request_proto = self.socket.recv_multipart(flags=zmq.NOBLOCK)
                    received = True
                except zmq.Again:
                    sleep(0.1)
                    continue

                self.logger.debug("i'm alive! event=%r, received=%r", self._event.is_set(), received)

                if not request_proto:
                    continue

                request = cast(
                    'AsyncMessageRequest',
                    jsonloads(request_proto[-1].decode()),
                )

                request_request_id = request.get('request_id', None)

                response: AsyncMessageResponse | None = None

                try:
                    if request['worker'] != self.identity:
                        message = f'got {request["worker"]}, expected {self.identity}'
                        raise RuntimeError(message)

                    if self.integration is None:
                        self.integration = self._create_integration(request)
                except Exception as e:
                    response = {
                        'request_id': str(request_request_id),
                        'worker': self.identity,
                        'response_time': 0,
                        'success': False,
                        'message': str(e),
                    }

                if response is None and self.integration is not None:
                    response = self.integration.handle(request)
                    response.update({'request_id': str(request_request_id)})

                    if response.get('action', None) in ['DISC', 'DISCONNECT']:
                        connected = False

                    if self._event.is_set():
                        response.update({'message': 'abort', 'success': False})

                response_proto = [
                    request_proto[0],
                    SPLITTER_FRAME,
                    jsondumps(response, cls=JsonBytesEncoder).encode(),
                ]

                self.socket.send_multipart(response_proto)
        except Exception:
            if not self._event.is_set():
                self.logger.exception('unhandled exception')


def create_router_socket(context: ztypes.Context) -> ztypes.Socket:
    socket = cast('ztypes.Socket', context.socket(zmq.ROUTER))
    socket.setsockopt(zmq.LINGER, 0)
    socket.setsockopt(zmq.ROUTER_HANDOVER, 1)

    return socket


def router(run_daemon: Event, logger: logging.Logger) -> None:  # noqa: C901, PLR0912, PLR0915
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
    workers_available: list[str] = []

    with ThreadPoolExecutor(max_workers=500) as executor:

        def spawn_worker() -> None:
            identity = str(uuid4())

            worker = Worker(context, identity, run_daemon)

            future = executor.submit(worker.run)
            workers.update({identity: (future, worker)})
            logger.info('spawned worker %s', identity)

        client_worker_map: dict[str, str] = {}
        worker_identifiers_map: dict[str, bytes] = {}

        spawn_worker()

        worker_id: str
        request_client_id: str | None
        request_request_id: str | None
        socks_polled = 0

        try:
            while not run_daemon.is_set():
                socks = dict(poller.poll(timeout=1000))

                # every 10 seconds ---------------^
                if socks_polled % 10 == 0:
                    logger.debug("i'm alive!")
                    socks_polled = 0

                socks_polled += 1

                if not socks:
                    continue

                if socks.get(backend) == zmq.POLLIN:
                    try:
                        backend_response = backend.recv_multipart(flags=zmq.NOBLOCK)
                        logger.debug('backend: received a message')
                    except zmq.Again:
                        sleep(0.1)
                        continue

                    if not backend_response:
                        continue

                    reply = backend_response[2:]
                    worker_id = backend_response[0].decode()

                    worker_identifiers_map.update({worker_id: reply[0]})

                    if reply[0] == LRU_READY.encode():
                        workers_available.append(worker_id)
                        waiting_for_worker = False
                        logger.debug('worker %s is available', worker_id)
                    else:
                        payload = jsonloads(reply[-1].decode())
                        request_request_id = payload.get('request_id', None)

                        if len(reply) > 0 and reply[0] is not None and payload.get('action', None) in ['DISC', 'DISCONNECT']:
                            del workers[worker_id]
                            del worker_identifiers_map[worker_id]

                            logger.debug('removed mappings for worker %s', worker_id)

                        frontend.send_multipart(reply)
                        logger.debug('backend: sent message %s from %s', request_request_id, worker_id)

                        if waiting_for_worker:
                            logger.debug('backend: waiting for worker LRU')
                            continue

                if socks.get(frontend) == zmq.POLLIN:
                    try:
                        frontend_response = frontend.recv_multipart(flags=zmq.NOBLOCK)
                        logger.debug('frontend: received a message')
                    except zmq.Again:
                        sleep(0.1)
                        continue

                    request_id = frontend_response[0]
                    payload = cast('AsyncMessageRequest', jsonloads(frontend_response[-1].decode()))

                    request_worker_id = payload.get('worker', None)
                    request_client_id = str(payload.get('client', None))
                    request_request_id = payload.get('request_id', None)
                    client_key: str | None = None

                    logger.debug(
                        'frontend: received message %s from %d to %s',
                        request_request_id,
                        request_client_id,
                        request_worker_id,
                    )

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
                        if len(workers_available) < 2:
                            spawn_worker()

                        worker_id = workers_available.pop()

                        if client_key is not None:
                            client_worker_map.update({client_key: worker_id})
                    else:
                        logger.debug('frontend: client %d is assigned worker %s', request_client_id, request_worker_id)
                        worker_id = request_worker_id

                    if payload.get('worker', None) is None:
                        payload['worker'] = worker_id

                    request = jsondumps(payload).encode()
                    backend_request = [worker_id.encode(), SPLITTER_FRAME, request_id, SPLITTER_FRAME, request]
                    backend.send_multipart(backend_request)

            logger.info('stopping')
            for identity, (future, worker) in workers.items():
                if not future.running():
                    continue

                # tell client that we aborted
                if worker.integration is not None:
                    response = {
                        'success': False,
                        'worker': identity,
                        'message': 'abort',
                    }

                    response_proto = [
                        worker_identifiers_map.get(identity),
                        SPLITTER_FRAME,
                        jsondumps(response, cls=JsonBytesEncoder).encode(),
                    ]

                    frontend.send_multipart(response_proto)

                    worker.logger.debug('sent abort to client')

                    worker.integration.close()
                    worker.socket.close()

                    # stop worker
                    cancelled = future.cancel()  # let's try at least...
                    logger.info('worker %s cancelled: %r', identity, cancelled)
                else:
                    # should complete when `worker.stop()` has had effect
                    futures.wait([future])

            logger.debug('destroy zmq context')
            context.destroy(linger=0)
        except Exception:
            if not run_daemon.is_set():
                logger.exception('unhandled exception for client %s, request id %s', request_client_id, request_request_id)

    logger.info('stopped')

    if not run_daemon.is_set():
        run_daemon.set()


def main() -> int:
    logger = logging.getLogger('router')
    run_daemon = Event()
    process = Process(target=router, args=(run_daemon, logger))

    def signal_handler(signum: int | Signals, _frame: FrameType | None) -> None:
        if run_daemon.is_set():
            return

        logger.info('received signal %r', signum)
        run_daemon.set()

    signal(SIGTERM, signal_handler)
    signal(SIGINT, signal_handler)

    proc.setproctitle('grizzly-async-messaged')  # set appl name on ibm mq

    try:
        # start router I/O loop
        process.start()

        # wait for event to be set, which would be in `signal_handler` or when router I/O loop ends
        run_daemon.wait()

        # this should terminate the process, by sending SIGTERM to it
        process.terminate()

        # wait up to 3 seconds (socket poll timeout is 1 second) for the process to finish,
        # ignore any exception that might happen during that time
        with suppress(Exception):
            process.join(timeout=3.0)

        # if the process is still alive after timeout, forcefully kill it
        if process.is_alive():
            logger.warning('killing process')
            process.kill()

    except KeyboardInterrupt:
        return 1
    else:
        return process.exitcode or 0
