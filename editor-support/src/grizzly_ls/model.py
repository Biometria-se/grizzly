from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Callable


@dataclass
class Step:
    keyword: str
    expression: str
    func: Callable[..., None]
    help: str | None = field(default=None)
