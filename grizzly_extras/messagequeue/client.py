import zmq

from json import dumps as jsondumps
from grizzly_extras.messagequeue import Context


def main() -> int:
    zmq_context = zmq.Context()
    client = zmq_context.socket(zmq.REQ)
    client.connect('tcp://127.0.0.1:5554')

    queue = 'IFKTEST'
    context: Context = {
        'queue_manager': 'BIOZ1QM',
        'connection': 'mq1.zeta.biometria.se',
        'channel': 'BIO.IPA.CONN',
        'username': 'mq_sca',
        'password': 'z4gL5Ae62spKIdnR',
        'key_file': '/workspaces/grizzly/mq_sca',
        'message_wait': 0,
    }

    client.send_json({
        'action': 'CONN',
        'context': context,
    })

    response = client.recv_json()

    if 'metadata' in response:
        del response['metadata']

    print(jsondumps(response, indent=2))

    worker = response['worker']

    client.send_json({
        'action': 'PUT',
        'worker': worker,
        'context': {
            'queue': queue,
        },
        'payload': 'hello from messagequeue-daemon',
    })

    response = client.recv_json()

    if 'metadata' in response:
        del response['metadata']

    print(jsondumps(response, indent=2))

    client.send_json({
        'action': 'GET',
        'worker': worker,
        'context': {
            'queue': queue,
        },
    })

    response = client.recv_json()

    if 'metadata' in response:
        del response['metadata']

    print(jsondumps(response, indent=2))

    while True:
        client.send_json({
            'action': 'GET',
            'worker': worker,
            'context': {
                'queue': queue,
            },
        })

        response = client.recv_json()
        success = response.get('success', False)
        error = response.get('error', None)
        if not success:
            print(f'{error=}')
            break
        else:
            print(response['payload'])

    return 0


if __name__ == '__main__':
    import sys
    sys.exit(main())

