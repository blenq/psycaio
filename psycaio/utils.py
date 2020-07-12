try:
    from asyncio import get_running_loop
except ImportError:  # pragma: no cover
    from asyncio import get_event_loop as get_running_loop  # noqa
from collections import defaultdict


class TypeCache(defaultdict):
    """ Cache object for types based on mixin """

    def __init__(self, prefix, mixin):
        self.prefix = prefix
        self.mixin = mixin

    def __missing__(self, cls):
        # create a new class that inherits from the mixin
        self[cls] = new_cls = type(
            self.prefix + cls.__name__, (self.mixin, cls), {})
        return new_cls
