# Test with as many asyncio loops as we can find on the current platform.
#
# For now that is the available loops in the stdlib and uvloop

import asyncio

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
    # so, not on Windows, use the default loop
    policies.append(("DefaultLoop", asyncio.DefaultEventLoopPolicy))

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
