"""Refoss asynchronous Python library for Home Assistant.

This library provides async support for Refoss smart devices using WebSocket.
"""

__version__ = "1.0.3"

from .common import ConnectionOptions, IpOrOptionsType, get_info, process_ip_or_options
from .exceptions import (
    DeviceConnectionError,
    DeviceConnectionTimeoutError,
    InvalidAuthError,
    MacAddressMismatchError,
    NotInitialized,
    RefossError,
    RpcCallError,
    WrongRefoss,
)
from .rpc_device.device import RpcDevice, RpcUpdateType

__all__ = [
    "ConnectionOptions",
    "DeviceConnectionError",
    "DeviceConnectionTimeoutError",
    "InvalidAuthError",
    "IpOrOptionsType",
    "MacAddressMismatchError",
    "NotInitialized",
    "RefossError",
    "RpcCallError",
    "RpcDevice",
    "RpcUpdateType",
    "WrongRefoss",
    "get_info",
    "process_ip_or_options",
]
