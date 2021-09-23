from . import AtomicVariable


class AtomicInteger(AtomicVariable[int]):
    __base_type__ = int
    pass
