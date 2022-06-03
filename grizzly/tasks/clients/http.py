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
from typing import Any

from . import client, ClientTask
from ...scenarios import GrizzlyScenario

import requests


@client('http', 'https')
class HttpClientTask(ClientTask):
    def get(self, parent: GrizzlyScenario) -> Any:
        with self.action(parent) as meta:
            url = parent.render(self.endpoint)

            response = requests.get(url)
            value = response.text
            parent.user._context['variables'][self.variable] = value
            meta['response_length'] = len(value)

    def put(self, parent: GrizzlyScenario) -> Any:
        return super().put(parent)
