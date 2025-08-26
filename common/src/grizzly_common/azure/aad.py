"""Non-grizzly implementation of an Azure Credential, used by grizzly."""

from __future__ import annotations

import json
import logging
import re
from base64 import b64decode, urlsafe_b64encode
from contextlib import suppress
from datetime import datetime, timezone
from enum import Enum
from hashlib import sha256
from html.parser import HTMLParser
from http.server import HTTPServer, SimpleHTTPRequestHandler
from os import environ
from pathlib import Path
from secrets import token_urlsafe
from threading import Thread
from typing import TYPE_CHECKING, Any, ClassVar, TypedDict, cast
from urllib.parse import parse_qs, urlparse
from uuid import uuid4

import requests
from azure.core.credentials import AccessToken, TokenCredential
from pyotp import TOTP
from requests.adapters import HTTPAdapter, Retry
from typing_extensions import Self

if TYPE_CHECKING:  # pragma: no cover
    from types import TracebackType

logger = logging.getLogger(__name__)


class AuthMethod(Enum):
    """Azure Entra ID authentication method enumeration.

    Defines the available authentication methods for Azure Entra ID flows.

    Attributes:
        NONE: No authentication required
        CLIENT: Client credentials flow (service-to-service authentication)
        USER: User authentication flow (interactive or delegated authentication)

    """

    NONE = 1
    CLIENT = 2
    USER = 3

    @classmethod
    def from_string(cls, value: str) -> AuthMethod:
        try:
            return cls[value.strip().upper()]
        except KeyError as e:
            message = f'"{value.upper()}" is not a valid value of {cls.__name__}'
            raise AssertionError(message) from e


class AuthType(Enum):
    """Azure Entra ID token delivery type enumeration.

    Defines how authentication tokens are delivered and stored in the authentication flow.

    Attributes:
        HEADER: Token delivered in HTTP Authorization header (standard OAuth2 flow)
        COOKIE: Token delivered and stored in HTTP cookies (browser-based flow)

    """

    HEADER = 1
    COOKIE = 2


class AzureAadError(Exception):
    """Base exception for Azure Entra ID authentication errors.

    Raised when errors occur during Azure Entra ID authentication flows.
    This serves as the parent exception for all Azure Entra ID-related errors in this module.

    """


class AzureAadFlowError(AzureAadError):
    """Exception raised for errors during Azure Entra ID authentication flows.

    Raised when authentication flow operations fail, such as during user or client
    authentication processes. This includes errors from unexpected HTTP status codes,
    service exceptions, token acquisition failures, and other flow-related issues.

    """


class CookieTokenPayload(TypedDict):
    """TypedDict representing token payload data from cookie-based authentication flows.

    This structure defines the expected fields in the token payload when using
    cookie-based authentication with Azure Entra ID. The data is typically extracted
    from HTML form posts during the authentication flow.

    Attributes:
        id_token: The JWT identity token containing user claims and authentication information
        client_info: Base64-encoded client information from the authentication response
        state: State parameter used to maintain request/response correlation and prevent CSRF attacks
        session_state: Session state information for managing authentication sessions

    """

    id_token: str | None
    client_info: str | None
    state: str | None
    session_state: str | None


class FormPostParser(HTMLParser):
    """HTML parser for extracting authentication tokens from form post responses.

    Parses HTML form responses from Azure Entra ID authentication flows to extract
    token payload data. The parser specifically looks for form action URLs and hidden
    input fields containing id_token, client_info, state, and session_state values
    that are used in cookie-based authentication flows.

    """

    action: str | None
    _payload: CookieTokenPayload

    def __init__(self) -> None:
        """Initialize the FormPostParser with empty payload data.

        Sets up the parser with None values for action URL and all payload fields
        (id_token, client_info, state, session_state).

        """
        super().__init__()

        self.action = None
        self._payload = {'id_token': None, 'client_info': None, 'state': None, 'session_state': None}

    @property
    def payload(self) -> CookieTokenPayload:
        """Get the extracted token payload data.

        Returns:
            CookieTokenPayload: Dictionary containing id_token, client_info, state, and session_state.

        Raises:
            AssertionError: If form action is missing or if any required payload properties are None.

        """
        assert self.action is not None, 'could not find form action attribute in response'
        missing_properties = ', '.join([key for key, value in self._payload.items() if value is None])
        assert len(missing_properties) == 0, f'not all properties was found: {missing_properties}'

        return self._payload

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        """Handle HTML start tags during parsing.

        Extracts form action URL from <form> tags and token values from <input> tags.
        For input tags, extracts name and value attributes for fields matching the
        expected payload keys (id_token, client_info, state, session_state).

        Args:
            tag: The HTML tag name.
            attrs: List of (attribute, value) tuples for the tag.

        """
        if tag == 'form':
            for attr, value in attrs:
                if attr == 'action':
                    self.action = value
        elif tag == 'input':
            prop_name: str | None = None
            prop_value: str | None = None

            for attr, value in attrs:
                if attr == 'name':
                    prop_name = value
                elif attr == 'value':
                    prop_value = value

            if prop_name is not None and prop_name in self._payload and prop_value is not None:
                self._payload.update({prop_name: prop_value})  # type: ignore[misc]


class AzureAadWebserver:
    """Temporary HTTP server for OAuth2 redirect handling during Azure Entra ID authentication.

    Manages a local HTTP server that acts as the OAuth2 redirect URI endpoint during
    user authentication flows. The server runs on localhost and captures the authorization
    code or token from the authentication response. This is only used when no custom
    redirect URI is provided and the credential is not being initialized.

    The server automatically starts when entering the context manager and stops when exiting,
    temporarily replacing the credential's redirect URI with its own localhost address.

    Attributes:
        enable: Whether the webserver should be activated. True when no redirect URI or initialize URL is provided.
        credential: The AzureAadCredential instance this webserver is associated with.

    """

    enable: bool
    credential: AzureAadCredential

    _http_server: HTTPServer
    _thread: Thread
    _redirect: str | None

    def __init__(self, credential: AzureAadCredential) -> None:
        """Initialize the webserver for OAuth2 redirect handling.

        Sets up the webserver configuration based on the credential's redirect URI and
        initialize URL settings. The webserver is enabled only when both redirect URI
        and initialize URL are None, indicating that a temporary localhost server is
        needed to capture the OAuth2 authorization response.

        Args:
            credential: The AzureAadCredential instance that this webserver will serve.
                The credential's redirect URI will be temporarily replaced with the
                webserver's localhost address during authentication.

        """
        self.credential = credential
        self.enable = self.credential.redirect is None and self.credential.initialize is None

    def _start(self) -> None:
        """Start the temporary HTTP server for OAuth2 redirect handling.

        Creates and starts an HTTP server on a random available port on localhost (127.0.0.1).
        The server runs in a daemon thread to avoid blocking the main thread. It's configured
        with a 0.5 second timeout and address reuse enabled. If the webserver is disabled
        (when redirect URI or initialize URL is already provided), this method returns immediately.

        The server captures OAuth2 authorization responses during the authentication flow.
        On Windows, OSError exceptions with 'WinError 10038' are suppressed as they occur
        during normal socket shutdown operations.

        """
        if not self.enable:
            return

        # start http server and do stuff here
        self._http_server = HTTPServer(
            ('127.0.0.1', 0),
            SimpleHTTPRequestHandler,
            bind_and_activate=False,
        )
        self._http_server.timeout = 0.5
        self._http_server.allow_reuse_address = True
        self._http_server.server_bind()
        self._http_server.server_activate()

        def serve_forever(httpd: HTTPServer) -> None:
            with httpd:
                try:
                    httpd.serve_forever()
                except OSError as e:
                    # will be thrown when closing the socket in disconnect, on windows only.
                    if 'WinError 10038' not in str(e):
                        raise

        self._thread = Thread(target=serve_forever, args=(self._http_server,))
        self._thread.daemon = True
        self._thread.start()

    def _stop(self) -> None:
        """Stop the temporary HTTP server and clean up resources.

        Closes the HTTP server socket and waits for the server thread to terminate
        (with a 1-second timeout). If the webserver is disabled, this method returns
        immediately without performing any cleanup.

        Any exceptions during thread joining are suppressed to ensure cleanup completes.

        """
        if not self.enable:
            return

        self._http_server.server_close()
        with suppress(Exception):
            self._thread.join(timeout=1)

    def __enter__(self) -> Self:
        """Enter the context manager and start the OAuth2 redirect server.

        Saves the current redirect URI, starts the HTTP server (if enabled), and
        temporarily replaces the credential's redirect URI with the localhost server
        address. This allows the OAuth2 flow to redirect to the local server for
        capturing the authorization code.

        Returns:
            Self: The webserver instance for context manager usage.

        """
        self._redirect = self.credential.redirect

        self._start()

        if self.enable:
            self.credential.redirect = f'http://localhost:{self._http_server.server_port}'

        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        """Exit the context manager and restore the original redirect URI.

        Stops the HTTP server and restores the credential's original redirect URI
        that was saved on entry. Returns True if no exception occurred during
        the context, False otherwise.

        Args:
            exc_type: The type of exception that occurred, if any.
            exc: The exception instance that occurred, if any.
            traceback: The traceback of the exception, if any.

        Returns:
            bool: True if no exception occurred, False otherwise.

        """
        self._stop()

        self.credential.redirect = self._redirect

        return exc is None


class AzureAadCredential(TokenCredential):
    """Azure Entra ID credential for authentication and token management.

    Implements Azure's TokenCredential interface to provide OAuth2-based authentication
    for Azure services. Supports both user authentication (interactive/delegated) and
    service principal authentication (client credentials flow). Handles token acquisition,
    refresh, MFA with TOTP, and both header-based and cookie-based authentication flows.

    This credential can authenticate against Azure Entra ID (formerly Azure Active Directory)
    using various flows including authorization code flow with PKCE, client credentials flow,
    and form-based authentication with cookie delivery.

    Attributes:
        COOKIE_NAME: The ASP.NET Core cookie name used for cookie-based authentication.
        provider_url_template: Template URL for Azure Entra ID OAuth2 endpoints.
        username: The username for user authentication flows (None for client auth).
        password: The password or client secret for authentication.
        scope: The OAuth2 scope for token requests.
        client_id: The Azure application (client) ID.
        tenant: The Azure tenant ID or URL.
        otp_secret: TOTP secret for MFA (optional).
        refresh_time: Token refresh interval in seconds (default: 3600).
        redirect: Custom redirect URI for OAuth2 flows (optional).
        initialize: Initial URL to start authentication from (optional).
        auth_type: Token delivery type (header or cookie).

    """

    COOKIE_NAME: ClassVar[str] = '.AspNetCore.Cookies'

    provider_url_template: ClassVar[str] = 'https://login.microsoftonline.com/{tenant}/oauth2/v2.0'

    username: str | None
    password: str | None
    scope: str | None
    client_id: str
    tenant: str | None
    otp_secret: str | None
    refresh_time: int = 3600

    redirect: str | None
    initialize: str | None

    auth_type: AuthType

    _access_token: AccessToken | None
    _webserver: AzureAadWebserver
    _refreshed: bool
    _token_payload: dict[str, Any] | None

    def __init__(
        self,
        username: str | None,
        password: str | None,
        tenant: str,
        auth_method: AuthMethod,
        /,
        host: str,
        redirect: str | None = None,
        initialize: str | None = None,
        otp_secret: str | None = None,
        scope: str | None = None,
        client_id: str | None = None,
    ) -> None:
        """Initialize Azure Entra ID credential for authentication.

        Sets up the credential with authentication parameters for either user-based or
        client-based authentication flows. Configures token delivery method (header or cookie)
        based on whether an initialize URL is provided.

        Args:
            username: Username for user authentication (None for client credentials flow).
            password: Password for user auth or client secret for service principal auth.
            tenant: Azure tenant ID, URL, or tenant identifier.
            auth_method: Authentication method (USER, CLIENT, or NONE).
            host: Base host URL for the service being authenticated against.
            redirect: Custom OAuth2 redirect URI (optional, defaults to temporary localhost).
            initialize: Initial URL to start authentication from for cookie-based flows (optional).
            otp_secret: TOTP secret for MFA authentication (optional, required if user has MFA enabled).
            scope: OAuth2 scope string for token requests (optional).
            client_id: Azure application (client) ID (optional, defaults to Azure CLI client ID).

        """
        self.username = username
        self.password = password
        self.tenant = tenant
        self.auth_method = auth_method

        self.host = host
        self.client_id = client_id or '04b07795-8ddb-461a-bbee-02f9e1bf7b46'
        """
        If `client_id` is not specified, the client id for `Azure Command Line Tool` will be used.
        """

        self.scope = scope
        self.redirect = redirect
        self.initialize = initialize
        self.otp_secret = otp_secret

        self._access_token = None

        self.auth_type = AuthType.HEADER if self.initialize is None else AuthType.COOKIE
        self._webserver = AzureAadWebserver(self)
        self._refreshed = False
        self._token_payload = None

    @property
    def access_token(self) -> AccessToken:
        """Get the current access token, acquiring a new one if needed.

        Returns the cached access token or acquires a new one if the cached token
        is expired or doesn't exist. This is a convenience property that calls
        get_token() with no arguments.

        Returns:
            AccessToken: The current valid access token with expiration timestamp.

        """
        return self.get_token()

    @property
    def refreshed(self) -> bool:
        """Check if the token was refreshed in the last get_token call.

        This is a one-time flag that returns True if the previous get_token() call
        resulted in a token refresh (due to expiration), then resets to False.
        Useful for tracking when tokens are renewed.

        Returns:
            bool: True if token was refreshed in last get_token call, False otherwise.

        """
        refreshed = self._refreshed
        self._refreshed = False

        return refreshed

    @property
    def webserver(self) -> AzureAadWebserver:
        """Get the OAuth2 redirect webserver instance.

        Returns the temporary HTTP server used for handling OAuth2 redirects during
        user authentication flows. The server is only activated when needed (no custom
        redirect URI or initialize URL provided).

        Returns:
            AzureAadWebserver: The webserver instance for OAuth2 redirect handling.

        """
        return self._webserver

    def get_tenant(self, tenant_id: str | None) -> str | None:
        """Extract and normalize the Azure tenant identifier.

        Resolves the tenant ID to use, either from the provided parameter or from
        the instance's tenant attribute. If the tenant is a URL, extracts the tenant
        ID from the URL path.

        Args:
            tenant_id: Optional tenant ID to use instead of the instance's tenant.

        Returns:
            str | None: The normalized tenant ID, or None if no tenant is configured.

        """
        tenant = tenant_id if tenant_id is not None else self.tenant

        if tenant is None:
            return None

        parsed_tenant = urlparse(tenant)
        if len(parsed_tenant.netloc) > 0:
            path = parsed_tenant.path.lstrip('/')
            tenant, _ = path.split('/', 1)

        return tenant

    def generate_log(self, file_name: str, name: str, response: requests.Response) -> None:
        """Generate detailed debug logs for authentication flow HTTP requests.

        Creates markdown-formatted logs of HTTP requests and responses during the
        authentication flow. Only generates logs when logger is at DEBUG level and
        GRIZZLY_CONTEXT_ROOT environment variable is set. Logs include request/response
        URLs, headers, and payloads for debugging authentication issues.

        Args:
            file_name: Path to the log file to append to.
            name: Descriptive name for this request/response in the log.
            response: The HTTP response object to log.

        """
        if logger.getEffectiveLevel() > logging.DEBUG or environ.get('GRIZZLY_CONTEXT_ROOT', None) is None:
            return

        with Path(file_name).open('a+') as fd:
            fd.write(f"""# {name} - `{response.request.method}: {response.status_code}`
## Request

### URL
```plain
{response.request.url}
```
### Headers
```plain
{json.dumps(dict(response.request.headers), indent=2)}
```

### Payload
```plain
{response.request.body!r}
```

## Response

### URL
```plain
{response.url}
```

### Headers
```plain
{json.dumps(dict(response.headers), indent=2)}
```

### Payload
```plain
{response.text}
```

""")

    def get_token(
        self,
        *scopes: str,
        claims: str | None = None,
        tenant_id: str | None = None,
        **_kwargs: Any,
    ) -> AccessToken:
        """Get an access token, acquiring a new one if expired or not yet obtained.

        Implements the TokenCredential interface method. Returns a cached token if still
        valid, or acquires a new token through the appropriate authentication flow (user
        or client credentials). Sets the refreshed flag when a token is renewed.

        Args:
            *scopes: OAuth2 scopes to request (uses instance scope if not provided).
            claims: Optional claims to request in the token (currently unused).
            tenant_id: Optional tenant ID to override the instance tenant.
            **_kwargs: Additional keyword arguments (ignored, for interface compatibility).

        Returns:
            AccessToken: A valid access token with expiration timestamp.

        Raises:
            AzureAadError: If authentication flow configuration is invalid.
            AzureAadFlowError: If authentication flow fails.

        """
        now = datetime.now(tz=timezone.utc).timestamp()

        if self.scope is not None and len(scopes) < 1:
            scopes += (self.scope,)

        if self._access_token is None or self._access_token.expires_on <= now:
            self._refreshed = self._access_token is not None and self._access_token.expires_on <= now

            if self.auth_method == AuthMethod.USER:
                with self.webserver:
                    self._access_token = self.get_oauth_authorization(
                        *scopes,
                        claims=claims,
                        tenant_id=tenant_id,
                    )
            else:
                self._access_token = self.get_oauth_token(tenant_id=tenant_id)

            logger.info('requested token for %s', self.username)

        return cast('AccessToken', self._access_token)

    def get_expires_on(self, token: str) -> int:
        """Extract the expiration timestamp from a JWT token.

        Decodes the JWT token payload to extract the 'exp' (expiration) claim.
        If decoding fails or the claim is missing, returns a default expiration
        of 3000 seconds from now.

        Args:
            token: The JWT token string to decode.

        Returns:
            int: Unix timestamp when the token expires.

        """
        # default to 3000 seconds
        default_exp = int(datetime.now(tz=timezone.utc).timestamp()) + 3000

        try:
            # header, payload, signature
            _, payload, _ = token.split('.', 2)

            # add padding, if there's more padding than needed b64decode will truncate it
            # minimal padding needed is ==
            payload = f'{payload}=='

            decoded = b64decode(payload)
            json_payload = json.loads(decoded)

            return cast('int', json_payload.get('exp', default_exp))
        except:
            logger.exception('failed to get expire timestamp from token')
            return default_exp

    def get_oauth_authorization(  # noqa: C901, PLR0912, PLR0915
        self,
        *scopes: str,
        claims: str | None = None,  # noqa: ARG002
        tenant_id: str | None = None,
    ) -> AccessToken:
        """Perform interactive user authentication flow to obtain an access token.

        Executes a complete OAuth2 authorization code flow with PKCE for user authentication.
        Supports both header-based (redirect URI) and cookie-based (initialize URI) token delivery.
        Handles multi-factor authentication using TOTP when configured. The flow includes:

        1. Initial authorization request
        2. Credential type verification
        3. Username and password submission
        4. MFA challenge with TOTP (if required)
        5. Authorization code or token retrieval
        6. Token exchange (for authorization code flow)

        Args:
            *scopes: OAuth2 scopes to request for the token.
            claims: Optional claims to request (currently unused, reserved for future use).
            tenant_id: Optional tenant ID to override the instance tenant.

        Returns:
            AccessToken: The obtained access token with expiration timestamp.

        Raises:
            AzureAadError: If neither initialize nor redirect URI is configured, or if
                both are configured simultaneously.
            AzureAadFlowError: If any step in the authentication flow fails, including:
                - Unexpected HTTP status codes
                - Service exceptions from Azure
                - Missing required MFA configuration
                - Invalid credentials
                - Missing required response fields

        """
        tenant = self.get_tenant(tenant_id)

        log_file = Path('flow.md')
        log_file.unlink(missing_ok=True)

        # disable logger for urllib3
        logging.getLogger('urllib3.connectionpool').setLevel(logging.CRITICAL)

        def _parse_response_config(response: requests.Response) -> dict[str, Any]:
            match = re.search(r'Config={(.*?)};', response.text, re.MULTILINE)

            if not match:
                message = f'no config found in response from {response.url}'
                raise ValueError(message)

            return cast('dict[str, Any]', json.loads(f'{{{match.group(1)}}}'))

        def update_state(
            state: dict[str, str],
            response: requests.Response,
        ) -> dict[str, Any]:
            config = _parse_response_config(response)

            for key in state:
                if key in config:
                    state[key] = str(config[key])
                elif key in response.headers:
                    state[key] = str(response.headers[key])
                else:
                    message = f'unexpected response body from {response.url}: missing "{key}" in config'
                    raise ValueError(message)

            return config

        def generate_uuid() -> str:
            uuid = uuid4().hex

            return f'{uuid[0:8]}-{uuid[8:12]}-{uuid[12:16]}-{uuid[16:20]}-{uuid[20:]}'

        def generate_pkcs() -> tuple[str, str]:
            code_verifier: bytes = urlsafe_b64encode(
                token_urlsafe(96)[:128].encode('ascii'),
            )

            code_challenge = urlsafe_b64encode(sha256(code_verifier).digest()).decode(
                'ascii',
            )[:-1]

            return code_verifier.decode('ascii'), code_challenge

        if self.initialize is None and self.redirect is None:
            message = 'neither initialize or redirect URIs has been set'
            raise AzureAadError(message)

        headers_ua: dict[str, str] = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0',
        }

        initialize_uri = self.initialize
        redirect_uri = self.redirect
        provider_url = self.provider_url_template.format(tenant=tenant)
        provider_parsed = urlparse(provider_url)

        is_token_v2_0: bool = True
        if initialize_uri is None:
            redirect_uri = cast('str', self.redirect)

        verify = True
        username_lowercase = cast('str', self.username).lower()

        with requests.Session() as session:
            retries = Retry(total=3, connect=3, read=3, status=0, backoff_factor=0.1)
            session.mount('https://', HTTPAdapter(max_retries=retries))

            headers: dict[str, str]
            payload: dict[str, Any]
            code_verifier: str | None = None
            code_challenge: str | None = None
            data: dict[str, Any]
            state: dict[str, str] = {
                'hpgact': '',
                'hpgid': '',
                'sFT': '',
                'sCtx': '',
                'apiCanary': '',
                'canary': '',
                'correlationId': '',
                'sessionId': '',
                'x-ms-request-id': '',
                'country': '',
            }

            # <!-- request 1
            if initialize_uri is None and redirect_uri is not None:
                # and redirect_uri is not None:
                client_id = self.client_id
                client_request_id = generate_uuid()

                redirect_uri_parsed = urlparse(redirect_uri)

                if len(redirect_uri_parsed.netloc) == 0:
                    redirect_uri = f'{self.host}{redirect_uri}'

                url = f'{provider_url}/authorize'

                params: dict[str, list[str]] = {
                    'response_type': ['id_token'],
                    'client_id': [client_id],
                    'redirect_uri': [redirect_uri],
                    'state': [generate_uuid()],
                    'client-request-id': [client_request_id],
                    'x-client-SKU': ['Js'],
                    'x-client-Ver': ['1.0.18'],
                    'nonce': [generate_uuid()],
                }

                if is_token_v2_0:
                    default_scopes = ['openid', 'profile', 'offline_access']
                    scope = ' '.join(scopes) if len(scopes) > 0 else ' '.join(default_scopes)

                    code_verifier, code_challenge = generate_pkcs()
                    params.update(
                        {
                            'response_mode': ['fragment'],
                            'response_type': ['code'],
                            'code_challenge_method': ['S256'],
                            'code_challenge': [code_challenge],
                            'scope': [scope],
                        },
                    )

                headers = {
                    'Host': provider_parsed.netloc,
                    **headers_ua,
                }

                response = session.get(url, headers=headers, params=params, allow_redirects=False)
            elif initialize_uri is not None and redirect_uri is None:
                initialize_uri_parsed = urlparse(initialize_uri)
                if len(initialize_uri_parsed.netloc) < 1:
                    initialize_uri = f'{self.host}{initialize_uri}'

                initialize_uri_parsed = urlparse(initialize_uri)

                headers = {
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                    'Accept-Encoding': 'gzip, deflate, br, zstd',
                    'Referer': 'https://login.microsoftonline.com/',
                    **headers_ua,
                }

                response = session.get(initialize_uri, headers=headers, verify=verify, allow_redirects=True)

                logger.debug('user auth request 0: %s (%d)', response.url, response.status_code)

                is_token_v2_0 = 'v2.0' in response.url
                if tenant is None:
                    response_parsed = urlparse(response.url)
                    tenant, _ = response_parsed.path.lstrip('/').split('/', 1)
                    provider_url = self.provider_url_template.format(tenant=tenant)
            else:
                message = 'both initialize and redirect URIs cannot be set'
                raise AzureAadError(message)

            logger.debug(
                'user auth request 1: %s (%d), is_token_v2_0=%r, provider_url=%s, initialize_uri=%s, redirect_uri=%s',
                response.url,
                response.status_code,
                is_token_v2_0,
                provider_url,
                initialize_uri,
                redirect_uri,
            )

            self.generate_log('flow.md', 'user auth request 1', response)

            if response.status_code != 200:
                message = f'user auth request 1: {response.url} had unexpected status code {response.status_code}'
                raise AzureAadFlowError(message)

            referer = response.url

            try:
                config = _parse_response_config(response)

                exception_message = config.get('strServiceExceptionMessage', None)

                if exception_message is not None and len(exception_message.strip()) > 0:
                    raise AzureAadFlowError(exception_message)

                config = update_state(state, response)
            except ValueError as e:
                raise AzureAadFlowError(str(e)) from None
            # // request 1 -->

            # <!-- request 2
            url_parsed = urlparse(config['urlGetCredentialType'])
            params = parse_qs(url_parsed.query)

            url = f'{url_parsed.scheme}://{url_parsed.netloc}{url_parsed.path}'
            host = url_parsed.netloc
            params['mkt'] = ['sv-SE']

            headers = {
                'Accept': 'application/json',
                'Accept-Encoding': 'gzip, deflate, br, zstd',
                'Host': host,
                'Origin': f'https://{host}',
                'Content-Type': 'application/json; charset=UTF-8',
                'canary': state['apiCanary'],
                'client-request-id': state['correlationId'],
                'hpgact': state['hpgact'],
                'hpgid': state['hpgid'],
                'hpgrequestid': state['sessionId'],
                **headers_ua,
            }

            payload = {
                'username': username_lowercase,
                'isOtherIdpSupported': True,
                'checkPhones': False,
                'isRemoteNGCSupported': True,
                'isCookieBannerShown': False,
                'isFidoSupported': True,
                'originalRequest': state['sCtx'],
                'country': state['country'],
                'forceotclogin': False,
                'isExternalFederationDisallowed': False,
                'isRemoteConnectSupported': False,
                'federationFlags': 0,
                'isSignup': False,
                'flowToken': state['sFT'],
                'isAccessPassSupported': True,
            }

            response = session.post(url, headers=headers, params=params, json=payload, allow_redirects=False)

            logger.debug('user auth request 2: %s (%d)', response.url, response.status_code)
            self.generate_log('flow.md', 'user auth request 2', response)

            if response.status_code != 200:
                message = f'user auth request 2: {response.url} had unexpected status code {response.status_code}'
                raise AzureAadFlowError(message)

            data = cast('dict[str, Any]', json.loads(response.text))
            if 'error' in data:
                error = data['error']
                message = f'error response from {url}: code={error["code"]}, message={error["message"]}'
                raise AzureAadFlowError(message)

            # update state with changed values
            state['apiCanary'] = data['apiCanary']
            state['sFT'] = data['FlowToken']
            # // request 2 -->

            # <!-- request 3
            url = config['urlPost'] if config['urlPost'].startswith('https://') else f'{url_parsed.scheme}://{host}{config["urlPost"]}'

            headers = {
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                'Accept-Encoding': 'gzip, deflate, br, zstd',
                'Cache-Control': 'max-age=0',
                'Content-Type': 'application/x-www-form-urlencoded',
                'Host': host,
                'Origin': f'https://{host}',
                'Referer': referer,
                **headers_ua,
            }

            payload = {
                'i13': '0',
                'login': username_lowercase,
                'loginfmt': username_lowercase,
                'type': '11',
                'LoginOptions': '3',
                'lrt': '',
                'lrtPartition': '',
                'hisRegion': '',
                'hisScaleUnit': '',
                'passwd': self.password,
                'ps': '2',  # postedLoginStateViewId
                'psRNGCDefaultType': '',
                'psRNGCEntropy': '',
                'psRNGCSLK': '',
                'canary': state['canary'],
                'ctx': state['sCtx'],
                'hpgrequestid': state['sessionId'],
                'flowToken': state['sFT'],
                'PPSX': '',
                'NewUser': '1',
                'FoundMSAs': '',
                'fspost': '0',
                'i21': '0',  # wasLearnMoreShown
                'CookieDisclosure': '0',
                'IsFidoSupported': '1',
                'isSignupPost': '0',
                'DfpArtifact': '',
                'i19': '16369',  # time on page
            }

            response = session.post(url, headers=headers, data=payload)

            logger.debug('user auth request 3: %s (%d)', response.url, response.status_code)
            self.generate_log('flow.md', 'user auth request 3', response)

            if response.status_code != 200:
                message = f'user auth request 3: {response.url} had unexpected status code {response.status_code}'
                raise AzureAadFlowError(message)

            if self.redirect is None or (self.redirect is not None and self.redirect not in response.url):
                config = _parse_response_config(response)

                exception_message = config.get('strServiceExceptionMessage', None)

                if exception_message is not None and len(exception_message.strip()) > 0:
                    logger.error(exception_message)
                    raise AzureAadFlowError(exception_message)

                config = update_state(state, response)

                referer = response.url

                user_proofs = config.get('arrUserProofs', [])

                if len(user_proofs) > 0:
                    otp_secret = self.otp_secret
                    otp_user_proofs = [
                        user_proof
                        for user_proof in user_proofs
                        if user_proof.get('authMethodId', None) == 'PhoneAppOTP' and 'SoftwareTokenBasedTOTP' in user_proof.get('phoneAppOtpTypes', [])
                    ]

                    if len(otp_user_proofs) != 1:
                        user_proof = user_proofs[0]

                        if otp_secret is None:
                            error_message = f'{username_lowercase} requires MFA for login: {user_proof["authMethodId"]} = {user_proof["display"]}'
                        else:
                            error_message = f'{username_lowercase} is assumed to use TOTP for MFA, but does not have that authentication method configured'

                        logger.error(error_message)

                        raise AzureAadFlowError(error_message)

                    if otp_secret is None:
                        message = f'{username_lowercase} requires TOTP for MFA, but auth.user.otp_secret is not set'
                        raise AzureAadFlowError(message)

                    # <!-- begin auth
                    poll_start = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
                    url = config['urlBeginAuth']

                    headers = {
                        'Canary': state['apiCanary'],
                        'Client-Request-Id': state['correlationId'],
                        'Hpgrequestid': state['x-ms-request-id'],
                        'Hpgact': state['hpgact'],
                        'Hpgid': state['hpgid'],
                        'Origin': host,
                        'Referer': referer,
                    }

                    payload = {
                        'AuthMethodId': 'PhoneAppOTP',
                        'Method': 'BeginAuth',
                        'ctx': state['sCtx'],
                        'flowToken': state['sFT'],
                    }

                    response = session.post(url, headers=headers, json=payload)

                    logger.debug('user auth request BeginAuth: %s (%d)', response.url, response.status_code)

                    if response.status_code != 200:
                        message = f'user auth request BeginAuth: {response.url} had unexpected status code {response.status_code}'
                        raise AzureAadFlowError(message)

                    payload = response.json()

                    if not payload['Success']:
                        error_message = f'user auth request BeginAuth: {payload.get("ErrCode", -1)} - {payload.get("Message", "unknown")}'
                        logger.error(error_message)
                        raise AzureAadFlowError(error_message)

                    state.update(
                        {
                            'sCtx': payload['Ctx'],
                            'sFT': payload['FlowToken'],
                            'correlationId': payload['CorrelationId'],
                            'sessionId': payload['SessionId'],
                            'x-ms-request-id': response.headers.get('X-Ms-Request-Id', state['x-ms-request-id']),
                        },
                    )
                    poll_end = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
                    # // begin auth -->

                    # <!-- end auth
                    totp = TOTP(otp_secret)
                    totp_code = totp.now()
                    url = config['urlEndAuth']
                    payload = {
                        'AdditionalAuthData': totp_code,
                        'AuthMethodId': 'PhoneAppOTP',
                        'Ctx': state['sCtx'],
                        'FlowToken': state['sFT'],
                        'Method': 'EndAuth',
                        'PollCount': 1,
                        'SessionId': state['sessionId'],
                    }

                    response = session.post(url, headers=headers, json=payload)
                    logger.debug('user auth request EndAuth: %s (%d)', response.url, response.status_code)

                    if response.status_code != 200:
                        message = f'user auth request EndAuth: {response.url} had unexpected status code {response.status_code}'
                        raise AzureAadFlowError(message)

                    payload = response.json()

                    if not payload['Success']:
                        error_message = f'user auth request EndAuth: {payload.get("ErrCode", -1)} - {payload.get("Message", "unknown")}'
                        logger.error(error_message)
                        raise AzureAadFlowError(error_message)

                    state.update(
                        {
                            'sCtx': payload['Ctx'],
                            'sFT': payload['FlowToken'],
                            'correlationId': payload['CorrelationId'],
                            'sessionId': payload['SessionId'],
                            'x-ms-request-id': response.headers.get('X-Ms-Request-Id', state['x-ms-request-id']),
                        },
                    )
                    # // end auth -->

                    # <!-- process auth
                    url = config['urlPost']

                    headers = {
                        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                        'Content-Type': 'application/x-www-form-urlencoded',
                        'Host': host,
                        'Referer': referer,
                        **headers_ua,
                    }

                    payload = {
                        'type': 19,
                        'GeneralVerify': False,
                        'request': state['sCtx'],
                        'mfaLastPollStart': poll_start,
                        'mfaLastPollEnd': poll_end,
                        'mfaAuthMethod': 'PhoneAppOTP',
                        'otc': int(totp_code),
                        'login': username_lowercase,
                        'flowToken': state['sFT'],
                        'hpgrequestid': state['x-ms-request-id'],
                        'sacxt': '',
                        'hideSmsInMfaProofs': False,
                        'canary': state['canary'],
                        'i19': 14798,
                    }

                    response = session.post(url, headers=headers, data=payload)
                    logger.debug('user auth request EndAuth: %s (%d)', response.url, response.status_code)

                    if response.status_code != 200:
                        message = f'user auth request ProcessAuth: {response.url} had unexpected status code {response.status_code}'
                        raise AzureAadFlowError(message)

                    try:
                        config = _parse_response_config(response)
                        exception_message = config.get('strServiceExceptionMessage', None)

                        if exception_message is not None and len(exception_message.strip()) > 0:
                            logger.error(exception_message)
                            raise AzureAadFlowError(exception_message)
                    except ValueError:  # pragma: no cover
                        pass

                    config = update_state(state, response)
                    # // process auth -->
                # // request 3 -->

                #  <!-- request 4
                if config['urlPost'].startswith('https://'):
                    message = f'unexpected response from {response.url}, incorrect username and/or password?'
                    raise AzureAadFlowError(message)

                url = f'https://{host}{config["urlPost"]}'

                headers = {
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                    'Accept-Encoding': 'gzip, deflate, br, zstd',
                    'Cache-Control': 'max-age=0',
                    'Content-Type': 'application/x-www-form-urlencoded',
                    'Origin': 'https://login.microsoftonline.com',
                    'Referer': referer,
                    **headers_ua,
                }

                payload = {
                    'LoginOptions': '3',
                    'type': '28',
                    'ctx': state['sCtx'],
                    'hprequestid': state['sessionId'],
                    'flowToken': state['sFT'],
                    'canary': state['canary'],
                    'i19': '1337',
                }

                # does not seem to be needed for token v2.0, so only add them for v1.0
                if not is_token_v2_0:
                    payload.update(
                        {
                            'i2': '',
                            'i17': '',
                            'i18': '',
                        },
                    )

                response = session.post(url, headers=headers, data=payload, allow_redirects=False)
                logger.debug('user auth request 4: %s (%d)', response.url, response.status_code)
                self.generate_log('flow.md', 'user auth request 4', response)

                if initialize_uri is None:
                    if response.status_code != 302:
                        try:
                            config = _parse_response_config(response)
                            exception_message = config.get('strServiceExceptionMessage', None)

                            if exception_message is not None and len(exception_message.strip()) > 0:
                                raise AzureAadFlowError(exception_message)
                        except ValueError:
                            pass

                        message = f'user auth request 4: {response.url} had unexpected status code {response.status_code}'
                        raise AzureAadFlowError(message)

                    if 'Location' not in response.headers:
                        message = f'Location header was not found in response from {response.url}'
                        raise AzureAadFlowError(message)

                    token_url = response.headers['Location']
                    if not token_url.startswith(f'{redirect_uri}'):
                        message = f'unexpected redirect URI, got {token_url} but expected {redirect_uri}'
                        raise AzureAadFlowError(message)
                    # // request 4 -->

                    token_url_parsed = urlparse(token_url)
                    fragments = parse_qs(token_url_parsed.fragment)

                    # exchange received with with a token
                    if is_token_v2_0:
                        assert code_verifier is not None, 'no code verifier has been generated!'
                        assert 'code' in fragments, f'could not find code in {token_url}'
                        code = fragments['code'][0]
                        return self.get_oauth_token(code=code, verifier=code_verifier, tenant_id=tenant)

                    if 'id_token' not in fragments:
                        message = f'could not find id_token in {token_url}'
                        raise AzureAadFlowError(message)

                    token = fragments['id_token'][0]
                    # be a little proactive and re-new set expire time to 10 minutes before actual time
                    expires_on = int(datetime.now(tz=timezone.utc).timestamp()) + (int(fragments.get('expires_in', ['3600'])[0]) - 600)

                    return AccessToken(token, expires_on)

                # token comes in the form of a cookie
                parser = FormPostParser()
                parser.feed(response.text)

                if response.status_code != 200 or parser.action is None:
                    try:
                        config = _parse_response_config(response)
                        exception_message = config.get('strServiceExceptionMessage', None)

                        if exception_message is not None and len(exception_message.strip()) > 0:
                            raise AzureAadFlowError(exception_message)
                    except ValueError:
                        pass

                    message = f'user auth request 4: {response.url} had unexpected status code {response.status_code}'
                    raise AzureAadFlowError(message)

                origin = f'https://{host}'

                headers = {
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                    'Accept-Encoding': 'gzip, deflate, br, zstd',
                    'Cache-Control': 'max-age=0',
                    'Origin': origin,
                    'Referer': origin,
                    **headers_ua,
                }

                with suppress(KeyError):
                    del headers['host']

                response = session.post(parser.action, headers=headers, data=parser.payload, allow_redirects=True, verify=verify)

                self.generate_log('flow.md', 'user auth request 5', response)

                if response.status_code != 200:
                    message = f'user auth request 5: {response.url} had unexpected status code {response.status_code}'
                    raise AzureAadFlowError(message)

                for cookie in session.cookies:
                    domain = cookie.domain[1:] if cookie.domain_initial_dot else cookie.domain

                    if domain in initialize_uri:
                        expires_on = (cookie.expires or int(datetime.now(tz=timezone.utc).timestamp() + 3600)) - 600
                        if cookie.value is None:
                            message = 'token cookie did not contain a value'
                            raise AzureAadFlowError(message)

                        self._token_payload = {key: getattr(cookie, key, None) for key in cookie.__dict__ if not key.startswith('_')}

                        return AccessToken(cookie.value, expires_on)

                message = 'did not find AAD cookie in authorization flow response session'
                raise AzureAadFlowError(message)

            # authenticated against a service principal in azure
            code_url_parsed = urlparse(response.url)
            fragments = parse_qs(code_url_parsed.fragment)

            if code_verifier is None:
                message = 'no code verifier has been generated!'
                raise AzureAadError(message)

            if 'code' not in fragments:
                message = f'could not find `code` in {response.url}'
                raise AzureAadFlowError(message)

            code = fragments['code'][0]

            return self.get_oauth_token(code=code, verifier=code_verifier)

    def get_oauth_token(
        self,
        *,
        code: str | None = None,
        verifier: str | None = None,
        resource: str | None = None,
        tenant_id: str | None = None,
    ) -> AccessToken:
        """Exchange authorization code for access token or perform client credentials flow.

        Obtains an access token by either:
        1. Exchanging an authorization code with PKCE verifier (user auth flow)
        2. Using client credentials (service principal auth flow)

        The method determines the flow based on the presence of code and verifier parameters.
        For authorization code flow, exchanges the code received from the authorization endpoint
        for an access token. For client credentials flow, authenticates using client ID and secret.

        Args:
            code: Authorization code from OAuth2 authorization flow (optional).
            verifier: PKCE code verifier corresponding to the authorization code (optional).
            resource: Resource/scope for client credentials flow (optional).
            tenant_id: Optional tenant ID to override the instance tenant.

        Returns:
            AccessToken: The obtained access token with expiration timestamp.

        Raises:
            AzureAadFlowError: If the token request fails or if neither id_token nor
                access_token is present in the response.

        """
        tenant = self.get_tenant(tenant_id)

        provider_url = self.provider_url_template.format(tenant=tenant)

        url = f'{provider_url}/token'

        # parameters valid for both versions
        parameters: dict[str, Any] = {
            'data': {'grant_type': None, 'client_id': self.client_id},
            'verify': True,
        }

        # build generic header values, but remove stuff that shouldn't be part
        # of authentication flow
        headers = {}

        if self.auth_type == AuthType.HEADER:
            redirect_uri = cast('str', self.redirect)
            redirect_uri_parsed = urlparse(redirect_uri)
        else:
            redirect_uri_parsed = urlparse(self.host)
            if len(redirect_uri_parsed.scheme) < 1:
                redirect_uri_parsed = redirect_uri_parsed._replace(scheme='https')

            if len(redirect_uri_parsed.netloc) < 1:
                redirect_uri_parsed = redirect_uri_parsed._replace(netloc=redirect_uri_parsed.path, path='')

        origin = f'{redirect_uri_parsed.scheme}://{redirect_uri_parsed.netloc}'

        headers.update({'Origin': origin, 'Referer': origin})

        if verifier is not None:
            parameters['data'].update(
                {
                    'grant_type': 'authorization_code',
                    'redirect_uri': redirect_uri,
                    'code': code,
                    'code_verifier': verifier,
                },
            )
        else:
            parameters['data'].update(
                {
                    'grant_type': 'client_credentials',
                    'client_secret': self.password,
                    'scope': resource,
                    'tenant': tenant,
                },
            )

        parameters.update({'headers': headers, 'allow_redirects': (code is None and verifier is None)})

        with requests.Session() as session:
            retries = Retry(total=3, connect=3, read=3, status=0, backoff_factor=0.1)
            session.mount('https://', HTTPAdapter(max_retries=retries))

            response = session.post(url, **parameters)
            payload = json.loads(response.text)

            if response.status_code != 200:
                raise AzureAadFlowError(payload['error_description'])

            token = payload.get('id_token', payload.get('access_token', None))
            if token is None:
                message = 'neither `id_token` or `access_token` was found in payload'
                raise AzureAadFlowError(message)

            expires_on = self.get_expires_on(token)

            self._token_payload = payload

            return AccessToken(token, expires_on)
