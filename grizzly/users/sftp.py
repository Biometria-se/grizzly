"""Communicates with Secure File Transport Protocol.

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
"""
from __future__ import annotations

from os import environ
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, Optional
from urllib.parse import urlparse

from grizzly.clients import SftpClientSession
from grizzly.types import GrizzlyResponse, RequestMethod
from grizzly.utils import merge_dicts

from .base import FileRequests, GrizzlyUser, ResponseHandler

if TYPE_CHECKING:  # pragma: no cover
    from paramiko import SFTPClient

    from grizzly.tasks import RequestTask
    from grizzly.types.locust import Environment


class SftpUser(ResponseHandler, GrizzlyUser, FileRequests):
    _auth_context: Dict[str, Any]

    _context_root: Path
    _payload_root: Path

    host: str
    port: int

    sftp_client: SftpClientSession
    session: SFTPClient

    def __init__(self, environment: Environment, *args: Any, **kwargs: Any) -> None:
        super().__init__(environment, *args, **kwargs)

        self.__class__._context = merge_dicts(super().context(), {'auth': {'username': None, 'password': None, 'key_file': None}})

        self._payload_root = Path(environ.get('GRIZZLY_CONTEXT_ROOT', '.')) / 'requests'
        self._download_root = self._payload_root / 'download'
        self._download_root.mkdir(exist_ok=True)

        parsed = urlparse(self.host or '')

        if parsed.scheme != 'sftp':
            message = f'{self.__class__.__name__}: "{parsed.scheme}" is not supported'
            raise ValueError(message)

        if parsed.username is not None or parsed.password is not None:
            message = f'{self.__class__.__name__}: username and password should be set via context variables "auth.username" and "auth.password"'
            raise ValueError(message)

        if len(parsed.path) > 0:
            message = f'{self.__class__.__name__}: only hostname and port should be included as host'
            raise ValueError(message)

        # should read key?
        if self._context['auth']['key_file'] is not None:
            message = f'{self.__class__.__name__}: key authentication is not implemented'
            raise NotImplementedError(message)

        host = parsed.netloc
        if ':' in host:
            [host, _] = host.split(':', 1)

        self.host = host
        self.port = int(parsed.port) if parsed.port is not None else 22
        username = self._context['auth']['username']
        password = self._context['auth']['password']
        key_file = self._context['auth']['key_file']

        if username is None:
            message = f'{self.__class__.__name__}: "auth.username" context variable is not set'
            raise ValueError(message)

        if password is None and key_file is None:
            message = f'{self.__class__.__name__}: "auth.password" or "auth.key" context variable must be set'
            raise ValueError(message)

    def on_start(self) -> None:
        """Create session when test starts."""
        super().on_start()
        self.sftp_client = SftpClientSession(self.host, self.port)
        self.session = self.sftp_client.session(**self._context['auth']).__enter__()

    def on_stop(self) -> None:
        """Close session when test stops."""
        self.session.__exit__(None, None, None)
        super().on_stop()

    def request_impl(self, request: RequestTask) -> GrizzlyResponse:
        """Execute SFTP request based on a request task."""
        payload: Optional[str] = None

        if request.method == RequestMethod.PUT:
            if request.source is None:
                message = f'{self.__class__.__name__}: request "{request.name}" does not have a payload, incorrect method specified'
                raise ValueError(message)

            payload = str((self._payload_root / request.source).resolve())
            self.session.put(payload, request.endpoint)
        elif request.method == RequestMethod.GET:
            file_name = Path(request.endpoint).name
            # @TODO: if endpoint is a directory, should we download all files in there?
            payload = str(self._download_root / file_name)
            self.session.get(request.endpoint, payload)
        else:
            message = f'{self.__class__.__name__} has not implemented {request.method.name}'
            raise NotImplementedError(message)

        headers = {
            'method': request.method.name.lower(),
            'host': self.host or '',
            'path': request.endpoint,
        }

        return (headers, payload)
