'''Communicates with Secure File Transport Protocol.

> **Warning**: Both local and remote files will be overwritten if they already exists. Downloaded files will be stored in `requests/download`.

## Request methods

Supports the following request methods:

* put
* get

## Format

Format of `host` is the following:

```plain
sftp://<host>[:<port>]
```

## Examples

Example of how to use it in a scenario:

```gherkin
Given a user of type "Sftp" load testing "sftp://sftp.example.com"
And set context variable "auth.username" to "bob"
And set context variable "auth.password" to "great-scott-42-file-bar"
Then put request "test/blob.file" to endpoint "/pub/blobs"
Then get request from endpoint "/pub/blobs/blob.file"
```
'''
from typing import Any, Dict, Tuple, Optional
from urllib.parse import urlparse
from time import monotonic as time
from os import path, environ, mkdir

from locust.exception import StopUser

from .meta import ContextVariables, FileRequests
from ..utils import merge_dicts
from ..clients import SftpClientSession
from ..types import RequestMethod
from ..task import RequestTask


class SftpUser(ContextVariables, FileRequests):
    _context: Dict[str, Any] = {
        'auth': {
            'username': None,
            'password': None,
            'key_file': None,
        }
    }

    _auth_context: Dict[str, Any]

    _context_root: str
    _payload_root: str

    client: SftpClientSession

    def __init__(self, *args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> None:
        super().__init__(*args, **kwargs)

        self._context = merge_dicts(super().context(), self.__class__._context)

        self._payload_root = path.join(environ.get('GRIZZLY_CONTEXT_ROOT', '.'), 'requests')
        self._download_root = path.join(self._payload_root, 'download')

        if not path.exists(self._download_root):
            mkdir(self._download_root)

        parsed = urlparse(self.host)

        if parsed.scheme != 'sftp':
            raise ValueError(f'{self.__class__.__name__}: "{parsed.scheme}" is not supported')

        if parsed.username is not None or parsed.password is not None:
            raise ValueError(f'{self.__class__.__name__}: username and password should be set via context variables "auth.username" and "auth.password"')

        if len(parsed.path) > 0:
            raise ValueError(f'{self.__class__.__name__}: only hostname and port should be included as host')

        # should read key?
        if self._context['auth']['key_file'] is not None:
            raise NotImplementedError(f'{self.__class__.__name__}: key authentication is not implemented')

        host = parsed.netloc
        if ':' in host:
            [host, _] = host.split(':', 1)

        port = int(parsed.port) if parsed.port is not None else 22
        username = self._context['auth']['username']
        password = self._context['auth']['password']
        key_file = self._context['auth']['key_file']

        if username is None:
            raise ValueError(f'{self.__class__.__name__}: "auth.username" context variable is not set')

        if password is None and key_file is None:
            raise ValueError(f'{self.__class__.__name__}: "auth.password" or "auth.key" context variable must be set')

        self.client = SftpClientSession(host, port)

    def request(self, request: RequestTask) -> None:
        request_name, endpoint, payload = self.render(request)

        name = f'{request.scenario.identifier} {request_name}'

        exception: Optional[Exception] = None
        response_length = 0
        start_time = time()

        try:
            def get_response_length(transferred: int, total: int) -> None:
                nonlocal response_length
                response_length = transferred

            with self.client.session(**self._context['auth']) as session:
                if request.method == RequestMethod.PUT:
                    if payload is None:
                        raise ValueError(f'{self.__class__.__name__}: request {name} does not have a payload, incorrect method specified')

                    payload = path.realpath(path.join(self._payload_root, payload))
                    session.put(payload, endpoint, get_response_length)
                elif request.method == RequestMethod.GET:
                    file_name = path.basename(endpoint)
                    # @TODO: if endpoint is a directory, should we download all files in there?
                    session.get(endpoint, path.join(self._download_root, file_name), get_response_length)
                else:
                    raise NotImplementedError(f'{self.__class__.__name__} has not implemented {request.method.name}')
        except Exception as e:
            exception = e
        finally:
            total_time = int((time() - start_time) * 1000)
            self.environment.events.request.fire(
                request_type=f'sftp:{request.method.name[:4]}',
                name=name,
                response_time=total_time,
                response_length=response_length,
                context=self._context,
                exception=exception,
            )

            if exception is not None and request.scenario.stop_on_failure:
                raise StopUser()
