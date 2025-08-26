"""All step implementations needed to write a feature file that describes a `locust` load test scenario for `grizzly`.

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

## Custom

Custom steps are implemented in your `grizzly` project `features/steps/steps.py` file. This is also the file that imports all `grizzly`-defined step implementations.

There are examples of this in the [example][example] documentation.

## Considerations

When writing step expressions, the following should be taken into consideration, regarding premuntations of code completion for step implementations.

::: grizzly_common.text
    options:
        show_root_heading: false
        show_root_toc_entry: false
        members:
        - PermutationEnum
        - permutation
"""

from __future__ import annotations

import parse
from grizzly_common.text import permutation

from grizzly.types import RequestDirection, RequestMethod
from grizzly.types.behave import register_type


@parse.with_pattern(r'(user[s]?)')
@permutation(vector=(False, True))
def parse_user_gramatical_number(text: str) -> str:
    return text.strip()


register_type(
    UserGramaticalNumber=parse_user_gramatical_number,
    Direction=RequestDirection.from_string,
    Method=RequestMethod.from_string,
)


from .background import *
from .scenario import *
from .setup import *
from .utils import *
