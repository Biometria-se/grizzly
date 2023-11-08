"""Abstract load user that handles logging request and responses."""
from __future__ import annotations

import json
import re
import traceback
import unicodedata
from datetime import datetime, timedelta
from os import environ
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, Optional, Union, cast
from urllib.parse import urlparse, urlunparse

from jinja2 import Template
from locust.clients import ResponseContextManager as RequestsResponseContextManager
from locust.contrib.fasthttp import ResponseContextManager as FastResponseContextManager

from grizzly.types import GrizzlyResponse, GrizzlyResponseContextManager, HandlerContextType, RequestDirection
from grizzly_extras.transformer import JsonBytesEncoder

from .response_event import ResponseEvent

if TYPE_CHECKING:  # pragma: no cover
    from requests import Response as RequestResponse

    from grizzly.tasks import RequestTask
    from grizzly.types.locust import Environment

    from .grizzly_user import GrizzlyUser

LOG_FILE_TEMPLATE = """[{{ request["time"] }}] -> {{ method }}{% if request["url"] != None %} {{ request["url"] }}{% endif %}:
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
"""  # noqa: E501


class RequestLogger(ResponseEvent):
    abstract: bool = True

    _context: Dict[str, Any]

    log_dir: Path

    def __init__(self, environment: Environment, *args: Any, **kwargs: Any) -> None:
        super().__init__(environment, *args, **kwargs)

        self.response_event.add_listener(self.request_logger)

        self.log_dir = Path(environ.get('GRIZZLY_CONTEXT_ROOT', '.')) / 'logs'

        log_dir_path = environ.get('GRIZZLY_LOG_DIR', None)
        if log_dir_path is not None:
            self.log_dir = self.log_dir / log_dir_path

        self.log_dir.mkdir(parents=True, exist_ok=True)

    @classmethod
    def _normalize(cls, value: str) -> str:
        value = unicodedata.normalize('NFKD', str(value)).encode('ascii', 'ignore').decode('ascii')
        value = re.sub(r'[^\w\s-]', '', value)

        return re.sub(r'[-\s]+', '-', value).strip('-_')

    @classmethod
    def _remove_secrets_attribute(cls, contents: Optional[Any]) -> Optional[Any]:
        if not isinstance(contents, dict):
            return contents

        for attribute in contents:
            if attribute in ['access_token', 'Authorization', 'authorization']:
                contents[attribute] = '*** REMOVED ***'

        return contents

    @classmethod
    def _get_http_user_data(cls, response: Union[GrizzlyResponseContextManager, RequestResponse]) -> Dict[str, Dict[str, Any]]:
        request_headers: Optional[Dict[str, Any]] = None
        request_body: Optional[str] = None

        response_body: Optional[str] = None
        response_headers: Optional[Dict[str, Any]] = None

        if response.text is not None:
            try:
                response_body = json.dumps(
                    cls._remove_secrets_attribute(
                        json.loads(response.text),
                    ),
                    indent=2,
                )
            except json.decoder.JSONDecodeError:
                response_body = str(response.text)
                if len(response_body.strip()) < 1:
                    response_body = None

        response_headers = dict(cast(Dict[str, Any], response.headers).items()) if len(response.headers or {}) > 0 else None

        request = response.request

        if request is not None:
            if isinstance(request.body, bytes):
                request_body = request.body.decode('utf-8')
            elif isinstance(request.body, str):
                request_body = request.body
            else:
                request_body = None

            request_headers = dict(request.headers.items()) if len(request.headers or {}) > 0 else None

            if request_body is not None:
                try:
                    request_body = json.dumps(
                        cls._remove_secrets_attribute(
                            json.loads(request_body),
                        ),
                        indent=2,
                    )
                except (json.decoder.JSONDecodeError, TypeError):
                    request_body = str(request_body)

        response_time: Optional[str]
        if hasattr(response, 'request_meta'):
            request_meta = getattr(response, 'request_meta', {})
            response_time = request_meta.get('response_time', None)
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

    @classmethod
    def _get_grizzly_response_user_data(
        cls,
        user: GrizzlyUser,
        request: RequestTask,
        context: HandlerContextType,
        exception: Optional[Exception],
        kwargs: Dict[str, Any],
    ) -> Dict[str, Any]:
        parsed = urlparse(user.host or '')
        sep = ''
        if (len(parsed.path) > 0 and parsed.path[-1] != '/' and request.endpoint[0] != '/') or (parsed.path == '' and request.endpoint[0] != '/'):
            sep = '/'

        parsed = parsed._replace(path=f'{parsed.path}{sep}{request.endpoint}')
        url = urlunparse(parsed)

        def unpack_context(response: HandlerContextType) -> GrizzlyResponse:
            if not isinstance(response, tuple):
                message = f'{type(response)} is not a GrizzlyResponse'
                raise TypeError(message)

            return cast(GrizzlyResponse, response)

        request_metadata: Optional[Dict[str, Any]] = None
        request_payload: Optional[str] = None
        response_metadata: Optional[Dict[str, Any]] = None
        response_payload: Optional[str] = None

        if request.method.direction == RequestDirection.TO:
            request_metadata, request_payload = unpack_context(context)
        elif request.method.direction == RequestDirection.FROM:
            response_metadata, response_payload = unpack_context(context)

        response_time = kwargs.get('locust_request_meta', {}).get('response_time', None)

        stacktrace: Optional[str] = None
        if exception is not None:
            stacktrace = ''.join(traceback.format_exception(
                type(exception),
                value=exception,
                tb=exception.__traceback__,
            ))

        return {
            'stacktrace': stacktrace,
            'request': {
                'time': None,
                'duration': None,
                'url': url,
                'metadata': request_metadata,
                'payload': request_payload,
            },
            'response': {
                'time': response_time,
                'url': None,
                'metadata': response_metadata,
                'payload': response_payload,
                'status': 'ERROR' if exception is not None else 'OK',
            },
        }

    def request_logger(
        self,
        name: str,
        context: HandlerContextType,
        request: RequestTask,
        user: GrizzlyUser,
        exception: Optional[Exception] = None,
        **kwargs: Dict[str, Any],
    ) -> None:
        """Handle `response_event` when fired."""
        if getattr(request, 'response', None) is None:
            return

        successful_request = (
            context.status_code in request.response.status_codes
            if isinstance(context, (RequestsResponseContextManager, FastResponseContextManager))
            else exception is None
        )

        if successful_request and not self.context().get('log_all_requests', False):
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

        if isinstance(context, (RequestsResponseContextManager, FastResponseContextManager)):
            variables.update(self._get_http_user_data(context))
        else:
            variables.update(self._get_grizzly_response_user_data(user, request, context, exception, kwargs))

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

        log_file = self.log_dir / log_name
        log_file.write_text(contents)
