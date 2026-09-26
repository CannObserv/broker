"""The canonical stream set, derived from co-core rather than listed.

co-core publishes no public collection of its topics (``_STREAM_KINDS`` is
private), but it does publish each one as a module-level constant in
``co_core.pure.adapters.bus.streams``. Reading those constants off the module
is derivation, not a mirror: a stream added upstream is in this set the moment
the pin moves, so a test written over it goes red until the broker has a row,
a group and a grant for it.

Both test trees used to hold a hand list instead, and co-core v0.19.6 added
``content.persist`` past both with every test green (CannObserv/broker#64).
"""

import co_core.pure.adapters.bus.streams as streams

CANONICAL_STREAMS: frozenset[str] = frozenset(
    value for name, value in vars(streams).items() if name.isupper() and isinstance(value, str)
)
