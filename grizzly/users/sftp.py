'''Communicates with Secure File Transport Protocol.

!!! attention
    Both local and remote files will be overwritten if they already exists. Downloaded files will be stored in `requests/download`.

## Request methods

Supports the following request methods:

* put
* get

## Format

Format of `host` is the following:

``` plain
sftp://<host>[:<port>]
```

## Examples

Example of how to use it in a scenario:

``` gherkin
Given a user of type "Sftp" load testing "sftp://sftp.example.com"
And set context variable "auth.username" to "bob"
And set context variable "auth.password" to "great-scott-42-file-bar"
Then put request "test/blob.file" to endpoint "/pub/blobs"
Then get request from endpoint "/pub/blobs/blob.file"
```
'''
from typing import Any, Dict, Tuple, Optional
from urllib.parse import urlparse
from os import path, environ, mkdir

from paramiko import SFTPClient

from grizzly.types import RequestMethod, GrizzlyResponse
from grizzly.types.locust import Environment
from grizzly.utils import merge_dicts
from grizzly.clients import SftpClientSession
from grizzly.tasks import RequestTask

from .base import GrizzlyUser, FileRequests, ResponseHandler


class SftpUser(ResponseHandler, GrizzlyUser, FileRequests):
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

    host: str
    port: int

    sftp_client: SftpClientSession
    session: SFTPClient

    def __init__(self, environment: Environment, *args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> None:
        super().__init__(environment, *args, **kwargs)

        self._context = merge_dicts(super().context(), self.__class__._context)

        self._payload_root = path.join(environ.get('GRIZZLY_CONTEXT_ROOT', '.'), 'requests')
        self._download_root = path.join(self._payload_root, 'download')

        if not path.exists(self._download_root):
            mkdir(self._download_root)

        parsed = urlparse(self.host or '')

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

        self.host = host
        self.port = int(parsed.port) if parsed.port is not None else 22
        username = self._context['auth']['username']
        password = self._context['auth']['password']
        key_file = self._context['auth']['key_file']

        if username is None:
            raise ValueError(f'{self.__class__.__name__}: "auth.username" context variable is not set')

        if password is None and key_file is None:
            raise ValueError(f'{self.__class__.__name__}: "auth.password" or "auth.key" context variable must be set')

    def on_start(self) -> None:
        super().on_start()
        self.sftp_client = SftpClientSession(self.host, self.port)
        self.session = self.sftp_client.session(**self._context['auth']).__enter__()

    def on_stop(self) -> None:
        self.session.__exit__(None, None, None)
        super().on_stop()

    def request_impl(self, request: RequestTask) -> GrizzlyResponse:
        payload: Optional[str] = None

        try:
            if request.method == RequestMethod.PUT:
                if request.source is None:
                    raise ValueError(f'{self.__class__.__name__}: request "{request.name}" does not have a payload, incorrect method specified')

                payload = path.realpath(path.join(self._payload_root, request.source))
                self.session.put(payload, request.endpoint)
            elif request.method == RequestMethod.GET:
                file_name = path.basename(request.endpoint)
                # @TODO: if endpoint is a directory, should we download all files in there?
                payload = path.join(self._download_root, file_name)
                self.session.get(request.endpoint, payload)
            else:
                raise NotImplementedError(f'{self.__class__.__name__} has not implemented {request.method.name}')

            headers = {
                'method': request.method.name.lower(),
                'host': self.host or '',
                'path': request.endpoint,
            }

            return (headers, payload,)
        except:
            raise
