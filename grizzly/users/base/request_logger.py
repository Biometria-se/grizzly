import os
import json
import unicodedata
import re
import traceback

from typing import Dict, Any, Tuple, Optional, cast
from datetime import datetime, timedelta
from urllib.parse import urlparse, urlunparse

from locust.env import Environment
from jinja2 import Template

from ...tasks import RequestTask
from ...types import GrizzlyResponse, HandlerContextType, RequestDirection, GrizzlyResponseContextManager
from ...utils import merge_dicts
from .response_event import ResponseEvent
from .grizzly_user import GrizzlyUser

from grizzly_extras.transformer import JsonBytesEncoder


LOG_FILE_TEMPLATE = '''
[{{ request["time"] }}] -> {{ method }}{% if request["url"] != None %} {{ request["url"] }}{% endif %}:
metadata:
{{ request["metadata"] if request["metadata"] != None else '<empty>' }}

payload:
{{ request["payload"] or '<empty>' }}

[{{ response["time"] }}] <- {% if response["url"] != None %}{{ response["url"] }} {% endif %}{% if request["duration"] != None %}({{ request["duration"] }} ms) {% endif %}status={{ response["status"] }}:
metadata:
{{ response["metadata"] if response["metadata"] != None else '<empty>' }}

payload:
{{ response["payload"] or '<empty>' }}
{%- if stacktrace != None %}

{{ stacktrace }}
{%- endif %}
'''.strip()  # noqa: E501


class RequestLogger(ResponseEvent, GrizzlyUser):
    abstract: bool = True

    log_dir: str

    _context: Dict[str, Any] = {
        'log_all_requests': False,
    }

    def __init__(self, environment: Environment, *args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> None:
        super().__init__(environment, *args, **kwargs)

        self.response_event.add_listener(self.request_logger)

        self.log_dir = os.path.join(os.environ.get('GRIZZLY_CONTEXT_ROOT', '.'), 'logs')
        if not os.path.exists(self.log_dir):
            os.mkdir(self.log_dir)

        self._context = merge_dicts(super().context(), RequestLogger._context)

    def _normalize(self, value: str) -> str:
        value = unicodedata.normalize('NFKD', str(value)).encode('ascii', 'ignore').decode('ascii')
        value = re.sub(r'[^\w\s-]', '', value)

        return re.sub(r'[-\s]+', '-', value).strip('-_')

    def _remove_secrets_attribute(self, contents: Optional[Any]) -> Optional[Any]:
        if not isinstance(contents, dict):
            return contents

        for attribute in contents:
            if attribute in ['access_token', 'Authorization', 'authorization']:
                contents[attribute] = '*** REMOVED ***'

        return contents

    def _get_http_user_data(self, response: GrizzlyResponseContextManager, user: GrizzlyUser) -> Dict[str, Dict[str, Any]]:
        request_headers: Optional[Dict[str, Any]] = None
        request_body: Optional[str] = None

        response_body: Optional[str] = None
        response_headers: Optional[Dict[str, Any]] = None

        if response.text is not None:
            try:
                response_body = json.dumps(
                    self._remove_secrets_attribute(
                        json.loads(response.text),
                    ),
                    indent=2,
                )
            except json.decoder.JSONDecodeError:
                response_body = str(response.text)
                if len(response_body.strip()) < 1:
                    response_body = None

        response_headers = {key: value for key, value in cast(Dict[str, Any], response.headers).items()} if len(response.headers or {}) > 0 else None

        request = response.request

        if request is not None:
            if isinstance(request.body, bytes):
                request_body = request.body.decode('utf-8')
            elif isinstance(request.body, str):
                request_body = request.body
            else:
                request_body = None

            request_headers = {key: value for key, value in request.headers.items()} if len(request.headers or {}) > 0 else None

            if request_body is not None:
                try:
                    request_body = json.dumps(
                        self._remove_secrets_attribute(
                            json.loads(request_body)
                        ),
                        indent=2,
                    )
                except (json.decoder.JSONDecodeError, TypeError):
                    request_body = str(request_body)

        response_time: Optional[str]
        if hasattr(response, 'request_meta') and response.request_meta is not None:
            response_time = response.request_meta.get('response_time', None)
        else:
            response_time = None

        return {
            'request': {
                'time': None,
                'duration': None,
                'url': (request or response).url,
                'metadata': request_headers,
                'payload': request_body,
            },
            'response': {
                'time': response_time,
                'url': response.url,
                'metadata': response_headers,
                'payload': response_body,
                'status': response.status_code,
            },
        }

    def request(self, request: RequestTask) -> GrizzlyResponse:
        return super().request(request)

    def request_logger(
        self,
        name: str,
        context: HandlerContextType,
        request: RequestTask,
        user: GrizzlyUser,
        exception: Optional[Exception] = None,
        **kwargs: Dict[str, Any],
    ) -> None:
        if getattr(request, 'response', None) is None:
            return

        successful_request = context.status_code in request.response.status_codes if isinstance(context, getattr(GrizzlyResponseContextManager, '__args__')) else exception is None

        if successful_request and not self._context.get('log_all_requests', False):
            return

        log_date = datetime.now()

        variables: Dict[str, Any] = {
            'method': request.method.name,
            'stacktrace': None,
            'request': {
                'time': None,
                'duration': None,
                'url': None,
                'metadata': None,
                'payload': None,
            },
            'response': {
                'time': None,
                'url': None,
                'status': None,
                'metadata': None,
                'payload': None,
            },
        }

        if isinstance(context, getattr(GrizzlyResponseContextManager, '__args__')):
            variables.update(self._get_http_user_data(context, user))
        else:
            parsed = urlparse(user.host or '')
            sep = ''
            if (len(parsed.path) > 0 and parsed.path[-1] != '/' and request.endpoint[0] != '/') or (parsed.path == '' and request.endpoint[0] != '/'):
                sep = '/'

            parsed = parsed._replace(path=f'{parsed.path}{sep}{request.endpoint}')
            url = urlunparse(parsed)

            variables['request'].update({
                'url': url,
            })

            def unpack_context(response: HandlerContextType) -> GrizzlyResponse:
                if not isinstance(response, tuple):
                    raise ValueError(f'{type(response)} is not a GrizzlyResponse')

                return cast(GrizzlyResponse, response)

            if request.method.direction == RequestDirection.TO:
                request_metadata: Optional[Dict[str, Any]]
                request_metadata, request_payload = unpack_context(context)

                variables['request'].update({
                    'metadata': request_metadata,
                    'payload': request_payload,
                })
            elif request.method.direction == RequestDirection.FROM:
                response_metadata: Optional[Dict[str, Any]]
                response_metadata, response_payload = unpack_context(context)

                variables['response'].update({
                    'metadata': response_metadata,
                    'payload': response_payload,
                })

            locust_request_meta = kwargs.get('locust_request_meta', None)
            if locust_request_meta is not None:
                variables['response']['time'] = locust_request_meta['response_time']

            if exception is not None:
                variables['stacktrace'] = ''.join(traceback.format_exception(
                    type(exception),
                    value=exception,
                    tb=exception.__traceback__,
                ))

            variables['response']['status'] = 'ERROR' if exception is not None else 'OK'

        response_time = variables['response'].get('time', None)
        if response_time is not None:
            variables['request']['duration'] = f'{variables["response"]["time"]:.2f}'
            if variables['request']['time'] is None:
                request_time = log_date - timedelta(milliseconds=response_time)
                variables['request']['time'] = f'{request_time}'
                variables['response']['time'] = f'{log_date}*'

        variables['response']['metadata'] = self._remove_secrets_attribute(variables['response']['metadata'])
        variables['request']['metadata'] = self._remove_secrets_attribute(variables['request']['metadata'])

        for v in ['response', 'request']:
            if variables[v]['metadata'] is not None:
                variables[v]['metadata'] = json.dumps(variables[v]['metadata'], indent=2, cls=JsonBytesEncoder)

            if variables[v]['time'] is None:
                variables[v]['time'] = f'{log_date}*'

        name = self._normalize(name)

        log_name = f'{name}.{log_date.strftime("%Y%m%dT%H%M%S%f")}.log'
        contents = Template(LOG_FILE_TEMPLATE).render(**variables)

        with open(os.path.join(self.log_dir, log_name), 'w') as fd:
            fd.write(contents)
