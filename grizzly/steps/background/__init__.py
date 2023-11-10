"""Module contains step implementations that only is allowed in the `Background` section in a `Feature`.

Steps in `Background` is only executed _once_. The feature will fail if they are added into any other section.

```gherkin
Feature: Example
    Background:
        # Here
    Scenario:
        # Not here!
```

They are only allowed in the `Background`-section since they modify parts of the context that are used for all scenarios, which can translate to basic `locust`
configuration.
"""

from .setup import *
from .shapes import *
