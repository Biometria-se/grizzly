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
        }
    })

    return 0
