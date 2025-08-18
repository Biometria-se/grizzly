"""Does nothing (noop).

This is useful for scenarios that collects and aggregates information from many different sources (targets) using
[client][grizzly.tasks.clients] tasks.

## Format

Format of `host` can be anything.

## Examples

```gherkin
Given a user of type "Dummy" load testing "/dev/null"
```
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from . import GrizzlyUser, grizzlycontext

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.tasks import RequestTask
    from grizzly.types import GrizzlyResponse
    from grizzly.types.locust import Environment


@grizzlycontext(context={})
class DummyUser(GrizzlyUser):
    def __init__(self, environment: Environment, *args: Any, **kwargs: Any) -> None:
        super().__init__(environment, *args, **kwargs)

    def request_impl(self, _: RequestTask) -> GrizzlyResponse:
        return None, None
