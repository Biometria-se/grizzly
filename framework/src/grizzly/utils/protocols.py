"""Helper methods for protocols that are used by both users
and tasks.
"""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from http.cookiejar import Cookie
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol, cast
from urllib.parse import urlparse

from async_messaged.utils import async_message_request
from dateutil.parser import ParserError
from dateutil.parser import parse as dateparser
from gevent.ssl import SSLContext, create_default_context

from grizzly.utils import _print_table

if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Callable
    from http.cookiejar import CookieJar

    from async_messaged import AsyncMessageRequest, AsyncMessageResponse
    from zmq import sugar as ztypes

    from grizzly.scenarios import GrizzlyScenario
    from grizzly.types.behave import Context


ALPN_PROTOCOLS = ['http/1.1']


class HttpCookieHolder(Protocol):
    cookiejar: CookieJar


def http_populate_cookiejar(holder: HttpCookieHolder, cookies: dict[str, str], *, url: str) -> None:
    parsed = urlparse(url)
    secure = parsed.scheme == 'https'
    domain = parsed.netloc

    holder.cookiejar.clear()

    for name, value in cookies.items():
        holder.cookiejar.set_cookie(
            Cookie(
                version=0,
                name=name,
                value=value,
                port=None,
                port_specified=False,
                domain=domain,
                domain_specified=True,
                domain_initial_dot=False,
                path='/',
                path_specified=True,
                secure=secure,
                expires=None,
                discard=False,
                comment=None,
                comment_url=None,
                rest={},
            ),
        )


def async_message_request_wrapper(parent: GrizzlyScenario, client: ztypes.Socket, request: AsyncMessageRequest) -> AsyncMessageResponse:
    """Wrap `grizzly_common.async_message.async_message_request` to make it easier to communicating with `async-messaged` from within `grizzly`."""
    request_string: str | None = None
    request_rendered: str | None = None

    try:
        request_string = json.dumps(request)
        request_rendered = parent.user.render(request_string)
        request = json.loads(request_rendered)
    except:
        parent.user.logger.error('failed to render request:\ntemplate=%r\nrendered=%r', request, request_rendered)  # noqa: TRY400
        raise

    if request.get('client', None) is None:
        request.update({'client': id(parent.user)})

    return async_message_request(client, request)


def zmq_disconnect(socket: ztypes.Socket, *, destroy_context: bool) -> None:
    socket.close(linger=0)
    if destroy_context:
        socket.context.destroy(linger=0)


def mq_client_logs(context: Context) -> None:
    """Check MQ logs (if available) for any errors that occured during a test, and present them in nice ASCII tables.

    ```bash
    $ pwd && ls -1
    /home/vscode/IBM/MQ/data/errors
    AMQ6150.0.FDC
    AMQERR01.LOG
    ```
    """
    if not hasattr(context, 'started'):
        return

    started = cast('datetime', context.started).astimezone(tz=timezone.utc)

    amqerr_log_entries: list[tuple[datetime, str]] = []
    amqerr_fdc_files: list[tuple[datetime, str]] = []

    log_directory = Path('~/IBM/MQ/data/errors').expanduser()

    # check errors files
    if not log_directory.exists():
        return

    for amqerr_log_file in log_directory.glob('AMQERR*.LOG'):
        with amqerr_log_file.open() as fd:
            line: str | None = None

            for line in fd:
                while line and not re.match(r'^\s+Time\(', line):
                    try:
                        line = next(fd)  # noqa: PLW2901
                    except StopIteration:  # noqa: PERF203
                        break

                if not line:
                    break

                try:
                    time_start = line.index('Time(') + 5
                    time_end = line.index(')')
                    time_str = line[time_start:time_end]
                    time_date = dateparser(time_str)

                    if time_date < started:
                        continue
                except (ParserError, ValueError):
                    continue

                while not line.startswith('AMQ'):
                    line = next(fd)  # noqa: PLW2901

                amqerr_log_entries.append((time_date, line.strip()))

    for amqerr_fdc_file in log_directory.glob('AMQ*.FDC'):
        modification_date = datetime.fromtimestamp(amqerr_fdc_file.stat().st_mtime).astimezone(tz=timezone.utc)

        if modification_date < started:
            continue

        amqerr_fdc_files.append((modification_date, str(amqerr_fdc_file)))

    # present entries created during run
    _print_table('AMQ error log entries', 'Message', amqerr_log_entries)
    _print_table('AMQ FDC files', 'File', amqerr_fdc_files)


def ssl_context_factory(cert: tuple[str, str] | None = None) -> Callable[[str | None], SSLContext]:
    def wrapper(*_args: Any, **_kwargs: Any) -> SSLContext:
        context = create_default_context()

        if cert is not None:
            client_cert, client_key = cert
            context.load_cert_chain(client_cert, client_key)

        context.set_alpn_protocols(ALPN_PROTOCOLS)

        return context

    return wrapper
