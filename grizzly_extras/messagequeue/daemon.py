from typing import List, cast
from uuid import uuid4
from json import loads as jsonloads, dumps as jsondumps
from threading import Thread

import setproctitle as proc

import zmq

from . import (
    MessageQueue,
    MessageQueueRequest,
    JsonBytesEncoder,
    LRU_READY,
    SPLITTER_FRAME,
    logger,
)

try:
    # not used here, but we should fail early if it's not installed
    import pymqi  # pylint: disable=unused-import
except ImportError:
    from .. import dummy_pymqi as pymqi


def router() -> None:
    proc.setproctitle('grizzly')
    logger.debug('router: starting')

    context = zmq.Context(1)
    frontend = context.socket(zmq.ROUTER)
    backend = context.socket(zmq.ROUTER)
    frontend.bind('tcp://127.0.0.1:5554')
    backend.bind('inproc://workers')

    poller = zmq.Poller()
    poller.register(frontend, zmq.POLLIN)
    poller.register(backend, zmq.POLLIN)

    def spawn_worker() -> None:
        identity = str(uuid4())

        thread = Thread(target=worker, args=(context, identity, ))
        thread.start()
        logger.info(f'router: spawned worker {identity} ({thread.ident})')

    workers_available: List[str] = []

    spawn_worker()

    while True:
        socks = dict(poller.poll())

        if socks.get(backend) == zmq.POLLIN:
            backend_response = backend.recv_multipart()
            if not backend_response:
                continue

            reply = backend_response[2:]
            if reply[0] != LRU_READY.encode():
                frontend.send_multipart(reply)
            else:
                worker_id = backend_response[0]
                logger.info(f'router: worker {worker_id.decode()} ready')
                workers_available.append(worker_id)

        if socks.get(frontend) == zmq.POLLIN:
            msg = frontend.recv_multipart()

            request_id = msg[0]
            payload = jsonloads(msg[-1].decode())

            worker_id = payload.get('worker', None)

            if worker_id is None:
                worker_id = workers_available.pop()
                payload['worker'] = worker_id.decode()
                logger.info(f'router: assigning worker {payload["worker"]}')
                request = jsondumps(payload).encode()
                if len(workers_available) == 0:
                    logger.debug(f'router: spawning an additional worker, for next client')
                    spawn_worker()
            else:
                worker_id = worker_id.encode()
                request = msg[-1]

            backend_request = [worker_id, SPLITTER_FRAME, request_id, SPLITTER_FRAME, request]
            backend.send_multipart(backend_request)


def worker(context: zmq.Context, identity: str) -> None:
    worker = context.socket(zmq.REQ)

    worker.setsockopt_string(zmq.IDENTITY, identity)
    worker.connect('inproc://workers')
    worker.send_string(LRU_READY)

    integration = MessageQueue(identity)

    while True:
        request_proto = worker.recv_multipart()
        if not request_proto:
            logger.error(f'{identity}: empty msg')
            continue

        request = cast(
            MessageQueueRequest,
            jsonloads(request_proto[-1].decode()),
        )

        if request['worker'] != identity:
            logger.error(f'got {request["worker"]}, expected {identity}')
            continue

        logger.debug(f'{identity}: send request to handler')
        response = integration.handler(request)
        logger.debug(f'{identity}: got response from handler')

        response_proto = [
            request_proto[0],
            SPLITTER_FRAME,
            jsondumps(response, cls=JsonBytesEncoder).encode(),
        ]

        worker.send_multipart(response_proto)


def main() -> int:
    if pymqi.__name__ == 'grizzly_extras.dummy_pymqi':
        raise NotImplementedError('pymqi not installed')
    try:
        router()
        return 0
    except KeyboardInterrupt:
        return 1
