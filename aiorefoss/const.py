"""Constants for refoss."""

from enum import Enum

import aiohttp

from .exceptions import DeviceConnectionError

CONNECT_ERRORS = (aiohttp.ClientError, DeviceConnectionError, OSError)


class UndefinedType(Enum):
    """Singleton type for use with not set sentinel values."""

    _singleton = 0


UNDEFINED = UndefinedType._singleton


# Timeout used for Device IO
DEVICE_IO_TIMEOUT = 10.0

# Timeout used for polling
DEVICE_POLL_TIMEOUT = 20.0

# Timeout used for initial connection calls
# after the connection has been established
DEVICE_INIT_TIMEOUT = 30.0


WS_HEARTBEAT = 30

# Notification sent by  device in case of WebSocket close
NOTIFY_WS_CLOSED = "NotifyWebSocketClosed"

DEFAULT_HTTP_PORT = 80
