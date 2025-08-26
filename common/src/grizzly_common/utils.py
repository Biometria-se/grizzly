"""Common utils."""

from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy


def merge_dicts(merged: dict, source: dict) -> dict:
    """Merge two dicts recursively, where `source` values takes precedance over `merged` values."""
    merged = deepcopy(merged)
    source = deepcopy(source)

    for key in source:
        if key in merged and isinstance(merged[key], dict) and (isinstance(source[key], Mapping) or source[key] is None):
            merged[key] = merge_dicts(merged[key], source[key] or {})
        else:
            value = source[key]
            if isinstance(value, str) and value.lower() == 'none':  # pragma: no cover
                value = None
            merged[key] = value

    return merged
