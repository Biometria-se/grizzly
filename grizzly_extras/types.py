from typing import Tuple, Optional


class PermutationVectored:
    __vector__: Optional[Tuple[bool, bool]]

    @property
    def vector(self) -> Optional[Tuple[bool, bool]]:
        return getattr(self, '__vector__', None)
