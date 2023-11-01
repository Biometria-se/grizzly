"""Grizzly specific clients user by grizzly users."""
from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Dict, Generator, Optional, Tuple, cast

from locust.clients import HttpSession, ResponseContextManager
from locust.event import EventHook
from paramiko import SFTPClient, Transport

if TYPE_CHECKING:  # pragma: no cover
    from locust.user.users import User
    from paramiko.pkey import PKey
    from urllib3 import PoolManager

    from .tasks import RequestTask


logger = logging.getLogger(__name__)


class ResponseEventSession(HttpSession):
    event_hook: EventHook

    def __init__(
        self,
        base_url: str,
        request_event: EventHook,
        user: Optional[User] = None,
        pool_manager: Optional[PoolManager] = None,
        *args: Tuple[Any, ...],
        **kwargs: Dict[str, Any],
    ) -> None:
        super().__init__(base_url, request_event, user, *args, pool_manager=pool_manager, **kwargs)

        self.event_hook = EventHook()

    def request(  # type: ignore  # noqa: PGH003, PLR0913
        self,
        method: str,
        url: str,
        name: Optional[str] = None,
        catch_response: bool = False,  # noqa: FBT001, FBT002
        context: Optional[Dict[str, Any]] = None,
        request: Optional[RequestTask] = None,
        **kwargs: Dict[str, Any],
    ) -> ResponseContextManager:
        """Override HttpSession.request to be able to fire grizzly specific event."""
        if context is None:
            context = {}

        response_context_manager = cast(
            ResponseContextManager,
            super().request(method, url, name, catch_response, context, **kwargs),
        )

        response_context_manager._entered = True

        self.event_hook.fire(
            name=name,
            request=request,
            context=response_context_manager,
            user=self.user,
        )

        return response_context_manager


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
        """Close open SFTP client, transport and reset session related properties."""
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
        """Create an SFTP session."""
        try:
            # there's no client, or username has changed -- create new client
            if self._client is None or username != self.username:
                self.close()

                if key_file is not None or key_file != self.key_file:
                    self.key_file = key_file
                    message = f'{self.__class__.__name__}: private key authentication is not supported'
                    raise NotImplementedError(message)

                self._transport = Transport((self.host, self.port))
                self._transport.connect(
                    None,
                    username,
                    password,
                    None,  # key needs to be converted to PKey
                )
                self._client = SFTPClient.from_transport(self._transport)

            if self._client is None:
                message = f'{self.__class__.__name__}: unknown error, there is no client'
                raise RuntimeError(message)

            yield self._client
        except Exception:
            self.close()

            raise
        else:
            self.username = username
