import csv

from typing import Dict, Any
from os import path
from time import perf_counter

import gevent

from flask import Flask, request, jsonify, Response as FlaskResponse, Request as FlaskRequest
from werkzeug.datastructures import Headers as FlaskHeaders


app = Flask(__name__)
root_dir = path.realpath(path.join(path.dirname(__file__), '..'))


@app.route('/api/v1/resources/dogs')
def app_get_dog_fact() -> FlaskResponse:
    _ = int(request.args.get('number', ''))

    return jsonify(['woof woof wooof'])


@app.route('/facts')
def app_get_cat_fact() -> FlaskResponse:
    _ = int(request.args.get('limit', ''))

    return jsonify(['meow meow meow'])


@app.route('/books/<book>.json')
def app_get_book(book: str) -> FlaskResponse:
    with open(f'{root_dir}/example/features/requests/books/books.csv', 'r') as fd:
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

    if app_request_count[x_grizzly_user] < nth - 1:
        status = wrong
        app_request_count[x_grizzly_user] += 1
    else:
        status = right
        app_request_count[x_grizzly_user] = 0

    return jsonify({attribute: status})


@app.errorhandler(404)
def catch_all(_: Any) -> FlaskResponse:
    return jsonify({}, status=200)
