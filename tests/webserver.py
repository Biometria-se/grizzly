"""Webserver used for end-to-end tests."""
from __future__ import annotations

import csv
import json
import logging
from pathlib import Path
from time import perf_counter
from typing import TYPE_CHECKING, Any, Dict, Literal, Optional, Type, cast

import gevent
from flask import Flask, jsonify, request
from flask import Request as FlaskRequest
from flask import Response as FlaskResponse
from gevent.pywsgi import WSGIServer
from werkzeug.datastructures import Headers as FlaskHeaders

if TYPE_CHECKING:  # pragma: no cover
    from types import TracebackType

    from grizzly.types import Self

logger = logging.getLogger('webserver')


auth_expected: Optional[Dict[str, Any]] = None


app = Flask('webserver')

# ugly hack to get correct path when webserver.py is injected for running distributed
root_dir = (Path(__file__).parent / '..').resolve()

if '/srv/grizzly' not in str(root_dir):
    root_dir = root_dir / 'example' / 'features'


@app.route('/api/v1/resources/dogs')
def app_get_dog_fact() -> FlaskResponse:
    logger.debug('route /api/v1/resources/dogs called')

    if len(request.get_data(cache=False, as_text=True)) > 0:
        return FlaskResponse(status=403)

    _ = int(request.args.get('number', ''))

    return jsonify(['woof woof wooof'])


@app.route('/facts')
def app_get_cat_fact() -> FlaskResponse:
    logger.debug('route /facts called')

    if len(request.get_data(cache=False, as_text=True)) > 0:
        return FlaskResponse(status=403)

    _ = int(request.args.get('limit', ''))

    return jsonify(['meow meow meow'])


@app.route('/books/<book>.json')
def app_get_book(book: str) -> FlaskResponse:
    logger.debug('/books/%s.json called, root_dir=%s', book, root_dir)

    if len(request.get_data(cache=False, as_text=True)) > 0:
        return FlaskResponse(status=403)

    with (root_dir / 'requests' / 'books' / 'books.csv').open() as fd:
        reader = csv.DictReader(fd)
        for row in reader:
            if row['book'] == book:
                return jsonify({
                    'number_of_pages': row['pages'],
                    'isbn_10': [row['isbn_10']] * 2,
                    'authors': [
                        {'key': '/author/' + row['author'].replace(' ', '_').strip() + '|' + row['isbn_10'].strip()},
                    ],
                })

    response = jsonify({'success': False})
    response.status_code = 500

    return response


@app.route('/author/<author_key>.json')
def app_get_author(author_key: str) -> FlaskResponse:
    logger.debug('route /author/%s.json called', author_key)
    name, _ = author_key.rsplit('|', 1)

    if len(request.get_data(cache=False, as_text=True)) > 0:
        return FlaskResponse(status=403)


    return jsonify({
        'name': name.replace('_', ' '),
    })


def get_headers(request: FlaskRequest) -> FlaskHeaders:
    return FlaskHeaders({key: value for key, value in request.headers.items() if key.lower() != 'content-length'})


@app.route('/api/statuscode/<statuscode>')
def app_statuscode(statuscode: int) -> FlaskResponse:
    response = jsonify({'message': 'a-okay!'})
    response.status_code = statuscode

    return response


@app.route('/api/echo', methods=['POST', 'PUT', 'GET'])
def app_echo() -> FlaskResponse:
    if request.method in ['POST', 'PUT']:
        payload = request.json
    elif request.method == 'GET':
        payload = {}

        for key, value in request.args.items():
            payload[key] = value

    if auth_expected is not None:
        try:
            assert request.headers.get('Authorization', '') == f'Bearer {auth_expected["token"]}', 'not authenticated'

            expected_headers = auth_expected.get('headers', None)
            if expected_headers is not None:
                for key, expected_value in expected_headers.items():
                    actual_value = request.headers.get(key, '')
                    assert actual_value == expected_value, f'header {key}: {actual_value} != {expected_value}'

        except AssertionError as e:
            response = jsonify({'message': str(e)})

            if str(e) == 'not authenticated':
                response.status_code = 401
            else:
                response.status_code = 403

            return response

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
        status_code = 400
    else:
        status = right
        app_request_count[x_grizzly_user] = 0
        status_code = 200

    json_result: Any = {attribute: status}

    if as_array is not None:
        json_result = [json_result]

    logger.debug('sending %r to %s', json.dumps(json_result), x_grizzly_user)

    response = jsonify(json_result)
    response.status_code = status_code

    return response


@app.route('/<tenant>/oauth2/v2.0/authorize')
def app_oauth2_authorize(tenant: str) -> FlaskResponse:
    return jsonify({'message': f'not implemented for {tenant}'}, status=400)


@app.route('/<tenant>/oauth2/v2.0/token', methods=['POST'])
def app_oauth2_token(tenant: str) -> FlaskResponse:
    form = request.form

    if auth_expected is None:
        response = jsonify({'error_description': 'not setup for authentication'})
        response.status_code = 400
        return response

    try:
        assert tenant == auth_expected['tenant'], f'{tenant} != {auth_expected["tenant"]}'
        assert form['grant_type'] == 'client_credentials', f'grant_type {form["grant_type"]} != client_credentials'
        assert form['client_secret'] == auth_expected['client']['secret'], f'client_secret {form["client_secret"]} != {auth_expected["client"]["secret"]}'
        assert form['client_id'] == auth_expected['client']['id'], f'client_id {form["client_id"]} != {auth_expected["client"]["id"]}'
    except AssertionError as e:
        response = jsonify({'error_description': str(e)})
        response.status_code = 400
        return response

    return jsonify({'access_token': auth_expected['token']})


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
        logger.debug('created webserver on port %d', port)

    @property
    def port(self) -> int:
        return cast(int, self._web_server.server_port)

    @property
    def auth(self) -> Optional[Dict[str, Any]]:
        return auth_expected

    @auth.setter
    def auth(self, value: Optional[Dict[str, Any]]) -> None:
        global auth_expected  # noqa: PLW0603
        auth_expected = value

    @property
    def auth_provider_uri(self) -> str:
        return '/oauth2/v2.0'

    def start(self) -> None:
        self._greenlet = gevent.spawn(lambda: self._web_server.serve_forever())
        gevent.sleep(0.01)
        logger.debug('started webserver on port %d', self.port)

    def __enter__(self) -> Self:
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

        logger.debug('stopped webserver on port %d', self.port)

        return True


if __name__ == '__main__':
    with Webserver(port=8080) as webserver:
        gevent.joinall([webserver._greenlet])
