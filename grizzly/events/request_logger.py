"""Abstract load user that handles logging request and responses."""
from __future__ import annotations

import json
import traceback
from datetime import datetime, timedelta
from os import environ
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, Optional
from urllib.parse import urlparse, urlunparse

from jinja2 import Template

from grizzly.events import GrizzlyEventHandler
from grizzly.utils import normalize
from grizzly_extras.transformer import JsonBytesEncoder

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.tasks import RequestTask
    from grizzly.types import GrizzlyResponse
    from grizzly.users import GrizzlyUser

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


class RequestLogger(GrizzlyEventHandler):
    _context: Dict[str, Any]

    log_dir: Path

    def __init__(self, user: GrizzlyUser) -> None:
        super().__init__(user)

        self.log_dir = Path(environ.get('GRIZZLY_CONTEXT_ROOT', '.')) / 'logs'

        log_dir_path = environ.get('GRIZZLY_LOG_DIR', None)
        if log_dir_path is not None:
            self.log_dir = self.log_dir / log_dir_path

        self.log_dir.mkdir(parents=True, exist_ok=True)

    @classmethod
    def _remove_secrets_attribute(cls, contents: Optional[Any]) -> Optional[Any]:
        if not isinstance(contents, dict):
            return contents

        for attribute in contents:
            if attribute in ['access_token', 'Authorization', 'authorization']:
                contents[attribute] = '*** REMOVED ***'

        return contents

    def _get_grizzly_response_user_data(
        self,
        request: RequestTask,
        context: GrizzlyResponse,
        exception: Optional[Exception],
        kwargs: Dict[str, Any],
    ) -> Dict[str, Any]:
        parsed = urlparse(self.user.host or '')
        sep = ''
        if (len(parsed.path) > 0 and parsed.path[-1] != '/' and request.endpoint[0] != '/') or (parsed.path == '' and request.endpoint[0] != '/'):
            sep = '/'

        parsed = parsed._replace(path=f'{parsed.path}{sep}{request.endpoint}')
        url = urlunparse(parsed)

        request_metadata: Optional[Dict[str, Any]] = None
        request_payload: Optional[str] = None
        response_metadata: Optional[Dict[str, Any]] = None
        response_payload: Optional[str] = None

        response_metadata, response_payload = context
        request_metadata = request.metadata
        request_payload = request.source

        if request_metadata == {}:
            request_metadata = None

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
                'url': url,
                'metadata': response_metadata,
                'payload': response_payload,
                'status': 'ERROR' if exception is not None else 'OK',
            },
        }

    def __call__(
        self,
        name: str,
        context: GrizzlyResponse,
        request: RequestTask,
        exception: Optional[Exception] = None,
        **kwargs: Any,
    ) -> None:
        if getattr(request, 'response', None) is None:
            return

        successful_request = exception is None

        if successful_request and not self.user.context().get('log_all_requests', False):
            return

        log_date = datetime.now().astimezone()

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

        variables.update(self._get_grizzly_response_user_data(request, context, exception, kwargs))

        response_time = variables['response'].get('time', None)
        if response_time is not None:
            variables['request']['duration'] = f'{variables["response"]["time"]:.2f}'
            if variables['request']['time'] is None:
                request_time = log_date - timedelta(milliseconds=response_time)
                variables['request']['time'] = f'{request_time}'
                variables['response']['time'] = f'{log_date.isoformat()}*'

        variables['response']['metadata'] = self._remove_secrets_attribute(variables['response']['metadata'])
        variables['request']['metadata'] = self._remove_secrets_attribute(variables['request']['metadata'])

        for v in ['response', 'request']:
            if variables[v]['metadata'] is not None:
                variables[v]['metadata'] = json.dumps(variables[v]['metadata'], indent=2, cls=JsonBytesEncoder)

            if variables[v]['time'] is None:
                variables[v]['time'] = f'{log_date.isoformat()}*'

        name = normalize(name)

        log_name = f'{name}.{log_date.strftime("%Y%m%dT%H%M%S%f")}.log'
        contents = Template(LOG_FILE_TEMPLATE).render(**variables)

        log_file = self.log_dir / log_name
        log_file.write_text(contents)
