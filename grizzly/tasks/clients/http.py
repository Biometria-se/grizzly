'''This task performs a HTTP request to a specified endpoint.

This is useful if the scenario is using a non-HTTP user or a request to a URL other than the one under testing is needed, e.g. for testdata.

Only supports `RequestDirection.FROM`.

## Step implementations

* {@pylink grizzly.steps.scenario.tasks.step_task_client_get_endpoint_payload}

* {@pylink grizzly.steps.scenario.tasks.step_task_client_get_endpoint_payload_metadata}

## Arguments

* `direction` _RequestDirection_ - only `RequestDirection.FROM` is implemented

* `endpoint` _str_ - URL to perform GET request from

* `name` _str_ - name used in `locust` statistics
'''
from typing import Optional, Dict, Any
from json import dumps as jsondumps

import requests

from locust.exception import CatchResponseError
from grizzly_extras.arguments import split_value, parse_arguments

from grizzly.types import GrizzlyResponse, RequestDirection, bool_type
from grizzly.scenarios import GrizzlyScenario
from grizzly.context import GrizzlyContextScenario

from . import client, ClientTask


@client('http', 'https')
class HttpClientTask(ClientTask):
    arguments: Dict[str, Any]
    headers: Dict[str, str]

    def __init__(
        self,
        direction: RequestDirection,
        endpoint: str,
        name: Optional[str] = None,
        /,
        payload_variable: Optional[str] = None,
        metadata_variable: Optional[str] = None,
        source: Optional[str] = None,
        destination: Optional[str] = None,
        text: Optional[str] = None,
        scenario: Optional[GrizzlyContextScenario] = None,
    ) -> None:
        verify = True

        if '|' in endpoint:
            endpoint, endpoint_arguments = split_value(endpoint)
            arguments = parse_arguments(endpoint_arguments, unquote=False)

            if 'verify' in arguments:
                verify = bool_type(arguments['verify'])
                del arguments['verify']

            if len(arguments) > 0:
                endpoint = f'{endpoint} | {", ".join([f"{key}={value}" for key, value in arguments.items()])}'

        super().__init__(
            direction,
            endpoint,
            name,
            payload_variable=payload_variable,
            metadata_variable=metadata_variable,
            source=source,
            destination=destination,
            scenario=scenario,
            text=text,
        )

        self.arguments = {}
        self.headers = {
            'x-grizzly-user': f'{self.__class__.__name__}::{id(self)}'
        }

        if self._scheme == 'https':
            self.arguments = {'verify': verify}

    def get(self, parent: GrizzlyScenario) -> GrizzlyResponse:
        with self.action(parent) as meta:
            url = parent.render(self.endpoint)

            meta.update({'request': {
                'url': url,
                'metadata': self.headers,
                'payload': None,
            }})

            response = requests.get(url, headers=self.headers, **self.arguments)
            payload = response.text
            metadata = dict(response.headers)

            if self.payload_variable is not None:
                parent.user._context['variables'][self.payload_variable] = payload

            if self.metadata_variable is not None:
                parent.user._context['variables'][self.metadata_variable] = jsondumps(metadata)

            meta['response_length'] = len(payload.encode('utf-8'))

            exception: Optional[Exception] = None

            if response.status_code != 200:
                exception = CatchResponseError(f'{response.status_code} not in [200]: {payload}')

            meta.update({
                'response': {
                    'url': response.url,
                    'metadata': metadata,
                    'payload': payload,
                    'status': response.status_code,
                },
                'exception': exception,
            })

            return metadata, payload

    def put(self, parent: GrizzlyScenario) -> GrizzlyResponse:
        raise NotImplementedError(f'{self.__class__.__name__} has not implemented PUT')
