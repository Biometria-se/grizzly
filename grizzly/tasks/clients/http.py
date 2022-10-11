'''This task performs a HTTP request to a specified endpoint.

This is useful if the scenario is using a non-HTTP user or a request to a URL other than the one under testing is needed, e.g. for testdata.

Only supports `RequestDirection.FROM`.

## Step implementations

* {@pylink grizzly.steps.scenario.tasks.step_task_client_get_endpoint}

## Arguments

* `direction` _RequestDirection_ - only `RequestDirection.FROM` is implemented

* `endpoint` _str_ - URL to perform GET request from

* `name` _str_ - name used in `locust` statistics
'''
from typing import Optional, Dict, Any

from grizzly_extras.arguments import split_value, parse_arguments

from . import client, ClientTask
from ...scenarios import GrizzlyScenario
from ...types import GrizzlyResponse, RequestDirection, bool_type
from ...context import GrizzlyContextScenario

import requests


@client('http', 'https')
class HttpClientTask(ClientTask):
    arguments: Dict[str, Any]

    def __init__(
        self,
        direction: RequestDirection,
        endpoint: str,
        name: Optional[str] = None,
        /,
        variable: Optional[str] = None,
        source: Optional[str] = None,
        destination: Optional[str] = None,
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

        super().__init__(direction, endpoint, name, variable=variable, source=source, destination=destination, scenario=scenario)

        self.arguments = {}

        if self._schema == 'https':
            self.arguments = {'verify': verify}

    def get(self, parent: GrizzlyScenario) -> GrizzlyResponse:
        with self.action(parent) as meta:
            url = parent.render(self.endpoint)

            response = requests.get(url, **self.arguments)
            value = response.text
            if self.variable is not None:
                parent.user._context['variables'][self.variable] = value
            meta['response_length'] = len(value)

            return dict(response.headers), value

    def put(self, parent: GrizzlyScenario) -> GrizzlyResponse:
        return super().put(parent)
