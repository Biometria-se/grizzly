'''Does nothing.

Can be used with any tasks except {@pylink grizzly.tasks.request}.

## Format

Format of `host` can be anything.

## Examples

Example of how to use it in a scenario:

``` gherkin
Given a user of type "Dummy" load testing "/dev/null"
```
'''
from typing import Any, Dict, Tuple, TYPE_CHECKING

from grizzly.types import GrizzlyResponse
from grizzly.types.locust import Environment
from grizzly.tasks import RequestTask

from .base import GrizzlyUser


if TYPE_CHECKING:  # pragma: no cover
    from grizzly.scenarios import GrizzlyScenario


class DummyUser(GrizzlyUser):
    def __init__(self, environment: Environment, *args: Tuple[Any], **kwargs: Dict[str, Any]) -> None:
        super().__init__(environment, *args, **kwargs)

    def request(self, parent: 'GrizzlyScenario', request: RequestTask) -> GrizzlyResponse:
        return None, None
