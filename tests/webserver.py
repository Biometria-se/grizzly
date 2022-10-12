import csv
import logging
import json

from typing import Dict, Any, Optional, Type, Literal, cast
from types import TracebackType
from time import perf_counter
from pathlib import Path

import gevent

from gevent.pywsgi import WSGIServer

from flask import Flask, request, jsonify, Response as FlaskResponse, Request as FlaskRequest
from werkzeug.datastructures import Headers as FlaskHeaders

logger = logging.getLogger('webserver')


app = Flask('webserver')

# ugly hack to get correct path when webserver.py is injected for running distributed
root_dir = (Path(__file__).parent / '..').resolve()
project_name = 'example'

if '.pytest_tmp' in __file__:
    root_dir = (root_dir / '..' / '..').resolve()
    project_name = 'test-example'


@app.route('/api/v1/resources/dogs')
def app_get_dog_fact() -> FlaskResponse:
    logger.debug('route /api/v1/resources/dogs called')
    _ = int(request.args.get('number', ''))

    return jsonify(['woof woof wooof'])


@app.route('/facts')
def app_get_cat_fact() -> FlaskResponse:
    logger.debug('route /facts called')
    _ = int(request.args.get('limit', ''))

    return jsonify(['meow meow meow'])


@app.route('/books/<book>.json')
def app_get_book(book: str) -> FlaskResponse:
    logger.debug(f'/books/{book}.json called')
    logger.debug(f'{root_dir=}, {project_name=}')
    with open(f'{root_dir}/{project_name}/features/requests/books/books.csv', 'r') as fd:
        reader = csv.DictReader(fd)
        for row in reader:
            if row['book'] == book:
                return jsonify({
                    'number_of_pages': row['pages'],
                    'isbn_10': [row['isbn_10']] * 2,
                    'authors': [
                        {'key': '/author/' + row['author'].replace(' ', '_').strip() + '|' + row['isbn_10'].strip()},
                    ]
                })


@app.route('/author/<author_key>.json')
def app_get_author(author_key: str) -> FlaskResponse:
    logger.debug(f'route /author/{author_key}.json called')
    name, _ = author_key.rsplit('|', 1)

    return jsonify({
        'name': name.replace('_', ' ')
    })


def get_headers(request: FlaskRequest) -> FlaskHeaders:
    return FlaskHeaders({key: value for key, value in request.headers.items() if not key.lower() == 'content-length'})


@app.route('/api/echo')
def app_echo() -> FlaskResponse:
    if request.method in ['POST', 'PUT']:
        payload = request.json
    elif request.method == 'GET':
        payload = {}

        for key, value in request.args.items():
            payload[key] = value

    headers = get_headers(request)

    response = jsonify(payload)
    response.headers = headers
    response.status_code = 200

    return response


@app.route('/api/sleep/<seconds>')
def app_sleep(seconds: str) -> FlaskResponse:
    start = perf_counter()
    gevent.sleep(float(seconds))
    took = int((perf_counter() - start) * 1000)

    response = jsonify({'slept': float(seconds), 'took': took})
    response.status_code = 200

    return response


app_request_count: Dict[str, int] = {}


@app.route('/api/until/reset')
def app_until_reset() -> FlaskResponse:
    x_grizzly_user = request.headers.get('x-grizzly-user', 'unknown')

    if x_grizzly_user in app_request_count:
        app_request_count[x_grizzly_user] = 0

    return jsonify({})


@app.route('/api/until/<attribute>')
def app_until_attribute(attribute: str) -> FlaskResponse:
    x_grizzly_user = request.headers.get('x-grizzly-user', 'unknown')

    if x_grizzly_user not in app_request_count:
        app_request_count[x_grizzly_user] = 0

    args = request.args
    nth = int(args['nth'])
    wrong = args.get('wrong')
    right = args.get('right')
    as_array = args.get('as_array', None)

    if app_request_count[x_grizzly_user] < nth - 1:
        status = wrong
        app_request_count[x_grizzly_user] += 1
    else:
        status = right
        app_request_count[x_grizzly_user] = 0

    json_result: Any = {attribute: status}

    if as_array is not None:
        json_result = [json_result]

    logger.debug(f'sending {json.dumps(json_result)} to {x_grizzly_user}')

    return jsonify(json_result)


@app.errorhandler(404)
def catch_all(_: Any) -> FlaskResponse:
    return jsonify({}, status=200)


class Webserver:
    _web_server: WSGIServer
    _greenlet: gevent.Greenlet

    def __init__(self, port: int = 0) -> None:
        self._web_server = WSGIServer(
            ('0.0.0.0', port),
            app,
            log=None,
        )
        logger.debug(f'created webserver on port {port}')

    @property
    def port(self) -> int:
        return cast(int, self._web_server.server_port)

    def start(self) -> None:
        self._greenlet = gevent.spawn(lambda: self._web_server.serve_forever())
        gevent.sleep(0.01)
        logger.debug(f'started webserver on port {self.port}')

    def __enter__(self) -> 'Webserver':
        self.start()

        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> Literal[True]:
        self._web_server.stop_accepting()
        self._web_server.stop()

        logger.debug(f'stopped webserver on port {self.port}')

        return True


if __name__ == '__main__':
    with Webserver(port=8080) as webserver:
        gevent.joinall([webserver._greenlet])
