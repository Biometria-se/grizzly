from contextlib import contextmanager
import logging

from typing import Dict, Optional, Any, List, Generator, Tuple
from uuid import uuid4
from json import loads as jsonloads, dumps as jsondumps, JSONEncoder
from threading import Thread

import setproctitle as proc

import zmq

logging.basicConfig(format="%(levelname)s: %(message)s", level=logging.DEBUG)

try:
    import pymqi
    has_dependency = True
except ImportError:
    has_dependency = False


LRU_READY = '\x01'
SPLITTER_FRAME = ''.encode()

logger = logging.getLogger(__name__)

class JsonBytesEncoder(JSONEncoder):
    def default(self, o: Any) -> Any:
        if isinstance(o, bytes):
            try:
                return o.decode('utf-8')
            except:
                return o.decode('latin-1')

        return JSONEncoder.default(self, o)


def router() -> None:
    proc.setproctitle('grizzly')

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

    workers: List[str] = []

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
                workers.append(worker_id)

        if socks.get(frontend) == zmq.POLLIN:
            msg = frontend.recv_multipart()


            request_id = msg[0]
            payload = jsonloads(msg[-1].decode())

            worker_id = payload.get('worker', None)

            if worker_id is None:
                worker_id = workers.pop()
                payload['worker'] = worker_id.decode()
                logger.info(f'router: assigning worker {payload["worker"]}')
                request = jsondumps(payload).encode()
                logger.debug(f'router: spawning an additional worker, for next client')
                spawn_worker()
            else:
                worker_id = worker_id.encode()
                request = msg[-1]

            backend_request = [worker_id, SPLITTER_FRAME, request_id, SPLITTER_FRAME, request]
            backend.send_multipart(backend_request)


def worker(context: zmq.Context, identity: str) -> None:
    @contextmanager
    def queue(_qmgr: pymqi.QueueManager, endpoint: str) -> Generator[pymqi.Queue, None, None]:
        logger.debug(f'{identity}: trying queue {endpoint}')
        queue = pymqi.Queue(_qmgr, endpoint)
        try:
            yield queue
        finally:
            queue.close()

    worker = context.socket(zmq.REQ)

    worker.setsockopt_string(zmq.IDENTITY, identity)
    worker.connect('inproc://workers')
    worker.send_string(LRU_READY)

    qmgr: Optional[pymqi.QueueManager] = None
    gmo: Optional[pymqi.GMO] = None
    md = pymqi.MD()
    get_arguments: Tuple[Any, ...]

    while True:
        msg = worker.recv_multipart()
        if not msg:
            logger.error(f'{identity}: empty msg')
            continue

        request = jsonloads(msg[-1].decode())
        if request['worker'] != identity:
            logger.error(f'got {request["worker"]}, expected {identity}')
            continue

        message = jsonloads(msg[2])

        reply: Dict[str, Any] = {
            'action': 'error',
            'success': True,
        }

        if message['action'] == 'CONN':
            if qmgr is not None:
                reply.update({
                    'success': False,
                    'error': 'already connected',
                })
            else:
                mq_context = message['context']
                key_file = mq_context.get('key_file', None)

                if key_file is not None:
                    cd = pymqi.CD(
                        ChannelName=mq_context['channel'].encode(),
                        ConnectionName=mq_context['connection'].encode(),
                        ChannelType=pymqi.CMQC.MQCHT_CLNTCONN,
                        TransportType=pymqi.CMQC.MQXPT_TCP,
                        SSLCipherSpec=mq_context['ssl_cipher'].encode(),
                    )

                    sco = pymqi.SCO(
                        KeyRepository=key_file.encode(),
                        CertificateLabel=mq_context['certificate_label'].encode(),
                    )

                    qmgr = pymqi.QueueManager(None)
                    qmgr.connect_with_options(
                        mq_context['queue_manager'],
                        user=mq_context['username'].encode(),
                        password=mq_context['password'].encode(),
                        cd=cd,
                        sco=sco,
                    )
                else:
                    qmgr = pymqi.connect(
                        mq_context['queue_manager'],
                        mq_context['channel'],
                        mq_context['connection'],
                        mq_context['username'],
                        mq_context['password'],
                    )

                get_arguments = (None, md)
                message_wait = mq_context.get('message_wait', None)
                if message_wait is not None and message_wait > 0:
                    gmo = pymqi.GMO(
                        Options=pymqi.CMQC.MQGMO_WAIT | pymqi.CMQC.MQGMO_FAIL_IF_QUIESCING,
                        WaitInterval=message_wait*1000,
                    )
                    get_arguments += (gmo, )

            logger.debug(f'{identity}: got CONN request')
            reply.update({
                'message': 'connected',
            })
        elif message['action'] == 'PUT':
            logger.debug(f'{identity}: got PUT request')
            if qmgr is None:
                reply.update({
                    'success': False,
                    'error': 'not connected',
                })
            else:
                with queue(qmgr, message['context']['queue']) as q:
                    payload = message.get('payload', None)
                    response_length = len(payload) if payload is not None else 0
                    q.put(payload, md)
                    reply.update({
                        'message': 'put message',
                        'response_length': response_length,
                        'metadata': md.get(),
                    })
        elif message['action'] == 'GET':
            logger.debug(f'{identity}: got GET request')
            if qmgr is None:
                reply.update({
                    'success': False,
                    'error': 'not connected',
                })
            else:
                with queue(qmgr, message['context']['queue']) as q:
                    try:
                        payload = q.get(*get_arguments).decode()
                        response_length = len(payload) if payload is not None else 0
                        reply.update({
                            'message': 'get message',
                            'response_length': response_length,
                            'metadata': md.get(),
                            'payload': payload,
                        })
                    except pymqi.MQMIError as e:
                        reply.update({
                            'success': False,
                            'error': str(e),
                        })
        else:
            reply.update({
                'success': False,
            })

        reply.update({
            'worker': identity,
            'action': request['action'],
        })

        response = [msg[0], SPLITTER_FRAME, jsondumps(reply, cls=JsonBytesEncoder).encode()]
        worker.send_multipart(response)


def main() -> int:
    try:
        router()
        return 0
    except KeyboardInterrupt:
        return 1