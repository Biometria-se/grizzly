'''This task performs a HTTP request to a specified endpoint.

This is useful if the scenario is using a non-HTTP user or a request to a URL other than the one under testing is needed, e.g. for testdata.

Instances of this task is created with the step expression, if endpoint is defined with scheme `http` or `https`:

* [`step_task_getter_of`](/grizzly/usage/steps/scenario/tasks/#step_task_get_endpoint)
'''
from typing import Callable, Any

import requests

from jinja2 import Template

from . import getterof, GetterOfTask
from ...context import GrizzlyScenarioBase


@getterof('http', 'https')
class HttpGetTask(GetterOfTask):
    def implementation(self) -> Callable[[GrizzlyScenarioBase], Any]:
        def _implementation(parent: GrizzlyScenarioBase) -> Any:
            with self.get(parent) as meta:
                url = Template(self.endpoint).render(**parent.user._context['variables'])

                response = requests.get(url)
                value = response.text
                parent.user._context['variables'][self.variable] = value
                meta['response_length'] = len(value)


        return _implementation
