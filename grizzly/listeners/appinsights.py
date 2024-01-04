"""@anchor pydoc:grizzly.listeners.appinsights Application Insights
This listener is based on the great work made by
[Svenska Spel and their Application Insights listener](https://github.com/SvenskaSpel/locust-plugins/blob/master/locust_plugins/appinsights_listener.py).

Minor changes, mainly related to adding typing, and also how InstrumentationKey is handled.

Example kusko question to get response time:

```kusko
traces
| extend response_time = todouble(customDimensions["response_time"]),
response_length = toint(customDimensions["response_length"]),
spawn_rate = tofloat(customDimensions["spawn_rate"]),
thread_count = toint(customDimensions["thread_count"]),
target_user_count = toint(customDimensions["target_user_count"]),
endpoint = tostring(customDimensions["endpoint"])
| project timestamp, endpoint, response_time, response_length, spawn_rate, thread_count
| render timechart;
```
"""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Dict, Optional
from urllib.parse import parse_qs, urlparse

from opencensus.ext.azure.log_exporter import AzureLogHandler

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.types.locust import Environment

stdlogger = logging.getLogger(__name__)


class ApplicationInsightsListener:
    def __init__(self, environment: Environment, url: str, *, propagate_logs: bool = True) -> None:
        url = url.replace(';', '&')
        parsed = urlparse(url)
        params = parse_qs(parsed.query)

        if parsed.hostname is None:
            assert 'IngestionEndpoint' in params, 'IngestionEndpoint was neither set as the hostname or in the query string'
            ingestion_endpoint = params['IngestionEndpoint'][0]
        else:
            ingestion_endpoint = f'https://{parsed.hostname}/'

        self.testplan = params['Testplan'][0] if 'Testplan' in params else 'appinsightstestplan'
        assert 'InstrumentationKey' in params, f'InstrumentationKey not found in {parsed.query}'
        instrumentation_key = params['InstrumentationKey'][0]
        self.environment = environment
        self.logger = logging.getLogger(f'{__name__}-azure')

        connection_string = f'InstrumentationKey={instrumentation_key};IngestionEndpoint={ingestion_endpoint}'

        self.logger.addHandler(AzureLogHandler(connection_string=connection_string))
        self.logger.propagate = propagate_logs

        environment.events.request.add_listener(self.request)

    def request(
        self,
        request_type: str,
        name: str,
        response_time: Any,
        response_length: int,
        exception: Optional[Any] = None,
        **_kwargs: Any,
    ) -> None:
        try:
            result = 'Success' if exception is None else 'Failure'

            if isinstance(response_time, float):
                response_time = int(round(response_time, 0))

            custom_dimensions = self._create_custom_dimensions_dict(
                request_type, result, response_time, response_length, name,
            )

            message_to_log = '{}: {} {} Response time: {} Number of Threads: {}'.format(
                result, str(request_type), str(name), str(response_time), custom_dimensions['thread_count'],
            )

            if exception is not None:
                message_to_log = f'{message_to_log} Exception: {exception!r}'

            self.logger.info(message_to_log, extra={'custom_dimensions': custom_dimensions})
        except:
            stdlogger.error('failed to write metric for "%s %s"', request_type, name)  # noqa: TRY400

    def _create_custom_dimensions_dict(
        self, method: str, result: str, response_time: int, response_length: int, endpoint: str, exception: Optional[Any] = None,
    ) -> Dict[str, Any]:
        return {
            'method': method,
            'result': result,
            'response_time': response_time,
            'response_length': response_length if response_length >= 0 else None,
            'endpoint': endpoint,
            'exception': str(exception) if exception is not None else None,
            'testplan': self.testplan,
        }
