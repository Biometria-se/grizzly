"""Module contains step implementations that only is allowed in the `Scenario` section in a feature file.

```gherkin
Feature: Example
    Background:
        # Not here!
    Scenario:
        # Here
```

The steps in the `Scenario` section modifies the context only for the scenario that they are defined in.
"""

from .response import *
from .results import *
from .setup import *
from .tasks import *
from .user import *
