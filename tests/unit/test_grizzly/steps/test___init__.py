"""Unit tests of grizzly.steps."""
from __future__ import annotations

from parse import compile

from grizzly.steps import parse_user_gramatical_number


def test_parse_user_gramatical_number() -> None:
    p = compile(
        'we have {user:d} {user_number:UserGramaticalNumber}',
        extra_types={'UserGramaticalNumber': parse_user_gramatical_number},
    )

    assert parse_user_gramatical_number.__vector__ == (False, True)

    assert p.parse('we have 1 user')['user_number'] == 'user'
    assert p.parse('we have 2 users')['user_number'] == 'users'
    assert p.parse('we have 3 people') is None
