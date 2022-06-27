
from typing import Tuple, Optional


class PermutationVectored:
    __vector__: Optional[Tuple[bool, bool]]

    @classmethod
    @property
    def vector(cls) -> Optional[Tuple[bool, bool]]:
        return getattr(cls, '__vector__', None)
