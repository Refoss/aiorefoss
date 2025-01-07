"""Refoss exceptions."""

from __future__ import annotations


class RefossError(Exception):
    """Base class for aioRefoss errors."""


class ConnectionClosed(RefossError):
    """Exception raised when the connection is closed."""


class InvalidMessage(RefossError):
    """Exception raised when an invalid message is received."""


class NotInitialized(RefossError):
    """Raised if device is not initialized."""


class WrongRefoss(RefossError):
    """Exception raised to indicate wrong Refoss generation."""


# Errors to be handled by the caller:
#    Errors that are expected to happen and should be handled by the caller.


class DeviceConnectionError(RefossError):
    """Exception indicates device connection errors."""


class DeviceConnectionTimeoutError(DeviceConnectionError):
    """Exception indicates device connection timeout errors."""


class InvalidAuthError(RefossError):
    """Raised to indicate invalid or missing authentication error."""


class MacAddressMismatchError(RefossError):
    """Raised if input MAC address does not match the device MAC address."""


class RpcCallError(RefossError):
    """Raised to indicate errors in RPC call."""

    def __init__(self, code: int, message: str = "") -> None:
        """Initialize JSON RPC errors."""
        self.code = code
        self.message = message
        super().__init__(code, message)
