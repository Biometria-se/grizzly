"""Does nothing.

Can be used with any tasks except {@pylink grizzly.tasks.request}.

## Format

Format of `host` can be anything.

## Examples

Example of how to use it in a scenario:

```gherkin
Given a user of type "Dummy" load testing "/dev/null"
```
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .base import GrizzlyUser

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.tasks import RequestTask
    from grizzly.types import GrizzlyResponse
    from grizzly.types.locust import Environment


class DummyUser(GrizzlyUser):
    def __init__(self, environment: Environment, *args: Any, **kwargs: Any) -> None:
        super().__init__(environment, *args, **kwargs)

    def request_impl(self, _: RequestTask) -> GrizzlyResponse:
        """Do nothing."""
        return None, None
