'''This task performs a HTTP request to a specified endpoint.

This is useful if the scenario is using a non-HTTP user or a request to a URL other than the one under testing is needed, e.g. for testdata.

Instances of this task is created with the step expression, if endpoint is defined with scheme `http` or `https`:

* [`step_task_client_get_endpoint`](/grizzly/framework/usage/steps/scenario/tasks/#step_task_client_get_endpoint)
'''
from typing import Any

from jinja2 import Template

from . import client, ClientTask
from ...scenarios import GrizzlyScenario

import requests


@client('http', 'https')
class HttpClientTask(ClientTask):
    def get(self, parent: GrizzlyScenario) -> Any:
        with self.action(parent) as meta:
            url = Template(self.endpoint).render(**parent.user._context['variables'])

            response = requests.get(url)
            value = response.text
            parent.user._context['variables'][self.variable] = value
            meta['response_length'] = len(value)

    def put(self, parent: GrizzlyScenario) -> Any:
        return super().put(parent)
