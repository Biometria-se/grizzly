"""Expose alls steps via grizzly.steps."""
from __future__ import annotations

import parse

from grizzly.types import RequestDirection, RequestMethod
from grizzly.types.behave import register_type
from grizzly_extras.text import permutation


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
