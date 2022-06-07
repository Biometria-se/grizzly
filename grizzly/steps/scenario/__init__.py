'''
@anchor pydoc:grizzly.steps.scenario Scenario
This package contains step implementations that only is allowed in the `Scenario` section in a feature file.

``` gherkin
Feature: Example
    Background:
        # Not here!
    Scenario:
        # Here
```

The steps in the `Scenario` section modifies the context only for the scenario that they are defined in.
'''

# flake8: noqa: F401,F403
from .tasks import *
from .setup import *
from .user import *
from .results import *
from .response import *
