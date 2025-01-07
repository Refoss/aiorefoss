"""Common code for Refoss library."""

from __future__ import annotations

import asyncio
import ipaddress
import logging
from dataclasses import dataclass
from socket import gethostbyname
from typing import Any, cast

from aiohttp import BasicAuth, ClientSession, ClientTimeout
from yarl import URL

from .const import (
    CONNECT_ERRORS,
    DEFAULT_HTTP_PORT,
    DEVICE_IO_TIMEOUT,
)
from .exceptions import (
    DeviceConnectionError,
    DeviceConnectionTimeoutError,
    MacAddressMismatchError,
)

_LOGGER = logging.getLogger(__name__)

DEVICE_IO_TIMEOUT_CLIENT_TIMEOUT = ClientTimeout(total=DEVICE_IO_TIMEOUT)


@dataclass
class ConnectionOptions:
    """Refoss options for connection."""

    ip_address: str
    username: str | None = None
    password: str | None = None
    device_mac: str | None = None
    port: int = DEFAULT_HTTP_PORT

    def __post_init__(self) -> None:
        """Call after initialization."""
        if self.username is not None:
            if self.password is None:
                raise ValueError("Supply both username and password")

            object.__setattr__(self, "auth", BasicAuth(self.username, self.password))


IpOrOptionsType = str | ConnectionOptions


async def process_ip_or_options(ip_or_options: IpOrOptionsType) -> ConnectionOptions:
    """Return ConnectionOptions class from ip str or ConnectionOptions."""
    if isinstance(ip_or_options, str):
        options = ConnectionOptions(ip_or_options)
    else:
        options = ip_or_options

    try:
        ipaddress.ip_address(options.ip_address)
    except ValueError:
        loop = asyncio.get_running_loop()
        options.ip_address = await loop.run_in_executor(
            None, gethostbyname, options.ip_address
        )

    return options


async def get_info(
    aiohttp_session: ClientSession,
    ip_address: str,
    device_mac: str | None = None,
    port: int = DEFAULT_HTTP_PORT,
) -> dict[str, Any]:
    """Get info from device through REST call."""
    error: (
        DeviceConnectionError | DeviceConnectionTimeoutError | MacAddressMismatchError
    )
    try:
        async with aiohttp_session.get(
            URL.build(
                scheme="http",
                host=ip_address,
                port=port,
                path="/rpc/Refoss.DeviceInfo.Get",
            ),
            raise_for_status=True,
            timeout=DEVICE_IO_TIMEOUT_CLIENT_TIMEOUT,
        ) as resp:
            result: dict[str, Any] = await resp.json()
    except TimeoutError as err:
        error = DeviceConnectionTimeoutError(err)
        _LOGGER.debug("host %s:%s: timeout error: %r", ip_address, port, error)
        raise error from err
    except CONNECT_ERRORS as err:
        error = DeviceConnectionError(err)
        _LOGGER.debug("host %s:%s: error: %r", ip_address, port, error)
        raise error from err
    mac = result["mac"]
    if device_mac and device_mac != mac:
        error = MacAddressMismatchError(f"Input MAC: {device_mac}, Refoss MAC: {mac}")
        _LOGGER.debug("host %s:%s: error: %r", ip_address, port, error)
        raise error

    return result


def fmt_macaddress(macaddress: str):
    """internal component macaddress representation (lowercase without dots/colons)"""
    return macaddress.replace(":", "").lower()


def mac_address_from_name(name: str) -> str | None:
    """Convert a name to a mac address."""
    mac = name.partition(".")[0].partition("-")[-1]
    return mac.lower() if len(mac) == 12 else None


def get_info_auth(info: dict[str, Any]) -> bool:
    """Return true if device has authorization enabled."""
    return cast(bool, info.get("auth_en"))
