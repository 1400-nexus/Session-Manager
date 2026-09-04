from typing import NewType

# Transport identities — a session key on the wire is a string, a receiver
# index is a small int.
SessionId = NewType("SessionId", str)
ReceiverId = NewType("ReceiverId", int)

# FEC identities — both are indices, never strings. Keeping them distinct
# NewTypes is what stops a block index being passed where a symbol index is
# expected (mypy --strict rejects it); keeping them both `int` is what stops
# the models that use them silently disagreeing with the declaration.
BlockId = NewType("BlockId", int)
SymbolId = NewType("SymbolId", int)
