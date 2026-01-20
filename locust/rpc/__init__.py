__all__ = (
    "Message",
    "rpc",
)

try:
    from . import zmqrpc as rpc
except ImportError:
    rpc = None  # type: ignore[assignment]  # ZMQ not available (e.g., on z/OS)

from .protocol import Message
