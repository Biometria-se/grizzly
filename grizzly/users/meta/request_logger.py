import os
import json
import unicodedata
import re
import traceback

from typing import Dict, Any, Tuple, Optional
from datetime import datetime, timedelta
from urllib.parse import urlparse, urlunparse

from locust.clients import ResponseContextManager
from jinja2 import Template

from ...task import RequestTask
from ...types import HandlerContextType, RequestDirection
from ...utils import merge_dicts
from .response_event import ResponseEvent
from .context_variables import ContextVariables

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
'''.strip()


class RequestLogger(ResponseEvent, ContextVariables):
    abstract = True

    log_dir: str

    _context: Dict[str, Any] = {
        'log_all_requests': False,
    }

    def __init__(self, *args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> None:
        super().__init__(*args, **kwargs)

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

        try:
            for attribute in ['access_token', 'Authorization', 'authorization']:
                if attribute in contents:
                    contents[attribute] = '*** REMOVED ***'
        except:
            pass
        finally:
            return contents

    def _get_http_user_data(self, response: ResponseContextManager) -> Dict[str, Dict[str, Any]]:
        request_body: Optional[str]
        response_body: Optional[str]

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

        try:
            if isinstance(response.request.body, bytes):
                request_body = response.request.body.decode('utf-8')
            elif isinstance(response.request.body, str):
                request_body = response.request.body
            else:
                request_body = None

            if request_body is not None:
                request_body = json.dumps(
                    self._remove_secrets_attribute(
                        json.loads(request_body)
                    ),
                    indent=2,
                )
        except (json.decoder.JSONDecodeError, TypeError):
            request_body = str(request_body)

        request_headers = dict(response.request.headers) if response.request.headers not in [None, {}] else None
        response_headers = dict(response.headers) if response.headers not in [None, {}] else None

        response_time: Optional[str]
        if hasattr(response, 'locust_request_meta'):
            response_time = response.locust_request_meta['response_time']
        else:
            response_time = None

        return {
            'request': {
                'time': None,
                'duration': None,
                'url': response.request.url,
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

    def request_logger(
        self,
        name: str,
        context: HandlerContextType,
        request: RequestTask,
        user: ContextVariables,
        exception: Optional[Exception] = None,
        **kwargs: Dict[str, Any],
    ) -> None:
        if getattr(request, 'response', None) is None:
            return

        successful_request = context.status_code in request.response.status_codes if isinstance(context, ResponseContextManager) else exception is None

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

        if isinstance(context, ResponseContextManager):
            variables.update(self._get_http_user_data(context))
        else:
            parsed = urlparse(user.host)
            sep = ''
            if (len(parsed.path) > 0 and parsed.path[-1] != '/' and request.endpoint[0] != '/') or (parsed.path == '' and request.endpoint[0] != '/'):
                sep = '/'

            parsed = parsed._replace(path = f'{parsed.path}{sep}{request.endpoint}')
            url = urlunparse(parsed)

            variables['request'].update({
                'url': url,
            })

            if request.method.direction == RequestDirection.TO:
                request_metadata: Optional[Dict[str, Any]]
                request_metadata, request_payload = context

                variables['request'].update({
                    'metadata': request_metadata,
                    'payload': request_payload,
                })
            elif request.method.direction == RequestDirection.FROM:
                response_metadata: Optional[Dict[str, Any]]
                response_metadata, response_payload = context

                variables['response'].update({
                    'metadata': response_metadata,
                    'payload': response_payload,
                })

            locust_request_meta = kwargs.get('locust_request_meta', None)
            if locust_request_meta is not None:
                variables['response']['time'] = locust_request_meta['response_time']

            if exception is not None:
                variables['stacktrace'] = ''.join(traceback.format_exception(
                    etype=type(exception),
                    value=exception,
                    tb=exception.__traceback__,
                ))

            variables['response']['status'] = f'ERROR' if exception is not None else 'OK'

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
