import logging

from typing import Optional, Union, Dict, Any, Tuple, Optional, Generator
from contextlib import contextmanager

from requests.models import Response
from locust.user.users import User
from locust.clients import ResponseContextManager, HttpSession
from locust.event import EventHook
from paramiko import SFTPClient, Transport
from paramiko.pkey import PKey

from .task import RequestTask


logger = logging.getLogger(__name__)

class ResponseEventSession(HttpSession):
    event_hook: EventHook

    def __init__(self, base_url: str, request_event: EventHook, user: Optional[User] = None, *args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> None:
        super().__init__(base_url, request_event, user, *args, **kwargs)

        self.event_hook = EventHook()

    def request(
        self,
        method: str,
        url: str,
        name: str,
        request: Optional[RequestTask] = None,
        catch_response: bool = False,
        **kwargs: Dict[str, Any],
    ) -> Union[ResponseContextManager, Response]:
        response = super().request(method, url, name, catch_response, **kwargs)

        self.event_hook.fire(
            name=name,
            request=request,
            context=response,
            user=self.user,
        )

        return response


class SftpClientSession:
    host: str
    port: int

    username: Optional[str]
    key_file: Optional[str]
    key: Optional[PKey]

    _transport: Optional[Transport]
    _client: Optional[SFTPClient]

    def __init__(self, host: str, port: int) -> None:
        self.host = host
        self.port = port
        self.username = None
        self.key = None
        self.key_file = None

        self._transport = None
        self._client = None

    def close(self) -> None:
        if self._client is not None:
            try:
                self._client.close()
            finally:
                self._client = None

        if self._transport is not None:
            try:
                self._transport.close()
            finally:
                self._transport = None

        self.username = None
        self.key_file = None
        self.key = None

    @contextmanager
    def session(self, username: str, password: str, key_file: Optional[str] = None) -> Generator[SFTPClient, None, None]:
        try:
            # there's no client, or username has changed -- create new client
            if self._client is None or username != self.username:
                self.close()

                if key_file is not None or key_file != self.key_file:
                    self.key_file = key_file
                    raise NotImplementedError(f'{self.__class__.__name__}: private key authentication is not supported')

                self._transport = Transport((self.host, self.port))
                self._transport.connect(
                    None,
                    username,
                    password,
                    None,  # key needs to be converted to PKey
                )
                self._client = SFTPClient.from_transport(self._transport)

            if self._client is None:
                raise RuntimeError(f'{self.__class__.__name__}: unknown error, there is no client')

            yield self._client
        except Exception as e:
            self.close()

            raise e
        else:
            self.username = username
