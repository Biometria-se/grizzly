'''This package contains all default step implementations needed to write a feature file that describes a `locust` load test scenario for `grizzly`.

A feature is described by using [Gherkin](https://cucumber.io/docs/gherkin/reference/). These expressions is then used by `grizzly` to configure and
start `locust`, which takes care of generating the load.

```gherkin
Feature: description of the test
    Background: steps that are common for all (if there is multiple) scenarios
        Given ...
        And ...
    Scenario: steps for a specific flow through a component in the target environment
        Given ...
        And ...
        Then ...
        When ...
```

In this package there are modules with step implementations that can be used in both `Background` and `Scenario` sections in a feature file.
'''

from .utils import *
from .setup import *
from .background import *
from .scenario import *
