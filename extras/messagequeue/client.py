import json
import zmq

from time import sleep


def main() -> int:
    context = zmq.Context()
    client = context.socket(zmq.REQ)
    client.connect('tcp://127.0.0.1:5554')

    client.send_json({
        'action': 'CONN',
        'context': {
            'queue_manager': '',
            'connection': '',
            'channel': '',
            'ssl_cipher': '',
            'key_file': '',
            'certificate_label': '',
            'username': '',
            'password': '',
            'message_wait': 10,
        }
    })

    reply = client.recv_json()

    worker = reply['worker']

    client.send_json({
        'action': 'PUT',
        'worker': worker,
        'context': {
            'queue': 'IFKTEST',
        },
        'payload': 'hello from messagequeue-daemon',
    })

    reply = client.recv_json()

    client.send_json({
        'action': 'GET',
        'worker': worker,
        'context': {
            'queue': 'IFKTEST',
        },
    })

    reply = client.recv_json()

    while True:
        client.send_json({
            'action': 'GET',
            'worker': worker,
            'context': {
                'queue': 'IFKTEST',
            },
        })

        reply = client.recv_json()
        success = reply.get('success', False)
        error = reply.get('error', None)
        if not success:
            print(f'{error=}')
            break
        else:
            print(reply['payload'])

    return 0
