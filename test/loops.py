# Test with as many asyncio loops as we can find on the current platform.
#
# For now that is the available loops in the stdlib and uvloop

import asyncio
from asyncio.proactor_events import BaseProactorEventLoop

try:
    from asyncio import get_running_loop
except ImportError:
    from asyncio import get_event_loop as get_running_loop

from psycaio.conn_proactor_connect import connect as proactor_connect

policies = []

# first try to get Windows policies
try:
    from asyncio import (
        WindowsSelectorEventLoopPolicy, WindowsProactorEventLoopPolicy)
    policies.extend([
        ("Selector", WindowsSelectorEventLoopPolicy),
        ("Proactor", WindowsProactorEventLoopPolicy),
        ])
except ImportError:
    pass

if not policies:
    # so, not on Windows, use the default (selector) loop
    policies.append(("DefaultLoop", asyncio.DefaultEventLoopPolicy))
    # The proactor version of psycaio is just delegating all blocking
    # operations to the default thread pool.
    # This is able to run fine on the selector loop as well, so we add this
    # policy to be able to test the proactor version on a non windows platform

    class ForcedProactorPolicy(asyncio.DefaultEventLoopPolicy):

        def new_event_loop(self):
            loop = super().new_event_loop()
            loop._proactor = "yes"
            return loop

    policies.append(("ForcedProactor", ForcedProactorPolicy))

# try to add uvloop as well, if available
try:
    import uvloop
except ImportError:
    pass
else:
    policies.append(("UVLoop", uvloop.EventLoopPolicy))


class ExplicitLoopMixin:

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        asyncio.set_event_loop_policy(cls.loop_policy())


def loop_classes(cls):
    for policy_name, policy in policies:
        # create a new class with the loop policy set
        new_cls = type(
            policy_name + cls.__name__, (ExplicitLoopMixin, cls), {})
        new_cls.loop_policy = policy
        yield new_cls


def uses_proactor():
    return hasattr(get_running_loop(), "_proactor")
