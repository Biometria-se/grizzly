"""Daemon implementation that handles are requests.
Based on ZMQ PUB/SUB, where each request type is handled to a worker which is for a specific client.
"""
from __future__ import annotations

import logging
from json import dumps as jsondumps
from json import loads as jsonloads
from signal import SIGINT, SIGTERM, Signals, signal
from threading import Thread
from time import sleep
from typing import TYPE_CHECKING, Dict, List, Optional, Union, cast
from urllib.parse import urlparse
from uuid import uuid4

import setproctitle as proc
import zmq.green as zmq

from grizzly_extras.transformer import JsonBytesEncoder

from . import (
    LRU_READY,
    SPLITTER_FRAME,
    AsyncMessageHandler,
    AsyncMessageRequest,
    AsyncMessageResponse,
)

if TYPE_CHECKING:  # pragma: no cover
    from types import FrameType

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


def router() -> None:  # noqa: C901, PLR0912, PLR0915
    logger = logging.getLogger('router')
    proc.setproctitle('grizzly-async-messaged')  # set appl name on ibm mq
    logger.debug('starting')

    context = zmq.Context(1)
    frontend = context.socket(zmq.ROUTER)
    backend = context.socket(zmq.ROUTER)
    frontend.bind('tcp://127.0.0.1:5554')
    backend.bind('inproc://workers')

    poller = zmq.Poller()
    poller.register(frontend, zmq.POLLIN)
    poller.register(backend, zmq.POLLIN)

    worker_threads: List[Thread] = []

    def spawn_worker() -> None:
        identity = str(uuid4())

        thread = Thread(target=worker, args=(context, identity))
        thread.daemon = True
        worker_threads.append(thread)
        thread.start()
        logger.info('spawned worker %s (%d)', identity, thread.ident)

    workers_available: List[str] = []

    client_worker_map: Dict[str, str] = {}

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

            reply = backend_response[2:]
            worker_id = backend_response[0].decode()

            if reply[0] != LRU_READY.encode():
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
    for worker_thread in worker_threads:
        logger.debug('waiting for %d', worker_thread.ident)
        worker_thread.join()

    try:
        context.destroy()
    except:
        logger.exception('failed to destroy zmq context')

    logger.info('stopped')


def worker(context: zmq.Context, identity: str) -> None:  # noqa: PLR0912, PLR0915
    logger = logging.getLogger(f'worker::{identity}')
    worker = context.socket(zmq.REQ)

    worker.setsockopt_string(zmq.IDENTITY, identity)
    worker.connect('inproc://workers')
    worker.send_string(LRU_READY)

    integration: Optional[AsyncMessageHandler] = None

    while not abort:
        try:
            request_proto = worker.recv_multipart(flags=zmq.NOBLOCK)
        except zmq.Again:
            sleep(0.1)
            continue

        logger.debug("i'm alive! abort=%r", abort)

        if not request_proto:
            logger.error('empty msg')
            continue

        request = cast(
            AsyncMessageRequest,
            jsonloads(request_proto[-1].decode()),
        )

        response: Optional[AsyncMessageResponse] = None

        try:
            if request['worker'] != identity:
                message = f'got {request["worker"]}, expected {identity}'
                raise RuntimeError(message)

            if integration is None:
                integration_url = request.get('context', {}).get('url', None)
                if integration_url is None:
                    message = 'no url found in request context'
                    raise RuntimeError(message)

                parsed = urlparse(integration_url)

                if parsed.scheme in ['mq', 'mqs']:
                    from .mq import AsyncMessageQueueHandler
                    integration = AsyncMessageQueueHandler(identity)
                elif parsed.scheme == 'sb':
                    from .sb import AsyncServiceBusHandler
                    integration = AsyncServiceBusHandler(identity)
                else:
                    message = f'integration for {parsed.scheme}:// is not implemented'
                    raise RuntimeError(message)
        except Exception as e:
            response = {
                'worker': identity,
                'response_time': 0,
                'success': False,
                'message': str(e),
            }

        if response is None and integration is not None:
            logger.debug('send request to handler')
            response = integration.handle(request)
            logger.debug('got response from handler')

        response_proto = [
            request_proto[0],
            SPLITTER_FRAME,
            jsondumps(response, cls=JsonBytesEncoder).encode(),
        ]

        worker.send_multipart(response_proto)

    logger.debug("i'm going to die! abort=%r", abort)
    logger.info('stopping')
    if integration is not None:
        logger.debug('closing %s', integration.__class__.__name__)
        try:
            integration.close()
        except:  # pragma: no cover
            logger.exception('failed to close integration')

    try:
        worker.close()
    except:  # pragma: no cover
        logger.exception('failed to close worker')
    logger.debug('stopped')


def main() -> int:
    try:
        router()
    except KeyboardInterrupt:
        return 1
    else:
        return 0
