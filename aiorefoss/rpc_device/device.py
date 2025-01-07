"""refoss  device."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Iterable
from enum import Enum, auto
from typing import Any, cast

from aiohttp import ClientSession

from ..common import (
    ConnectionOptions,
    IpOrOptionsType,
    process_ip_or_options,
)
from ..const import (
    CONNECT_ERRORS,
    DEVICE_INIT_TIMEOUT,
    DEVICE_IO_TIMEOUT,
    DEVICE_POLL_TIMEOUT,
    NOTIFY_WS_CLOSED,
)
from ..exceptions import (
    DeviceConnectionError,
    InvalidAuthError,
    MacAddressMismatchError,
    NotInitialized,
    RpcCallError,
    RefossError,
    WrongRefoss,
)

from .wsrpc import RPCSource, WsRPC

_LOGGER = logging.getLogger(__name__)


def mergedicts(dest: dict, source: dict) -> None:
    """Deep dicts merge."""
    for k, v in source.items():
        if k in dest and type(v) is dict:  # - only accepts `dict` type
            mergedicts(dest[k], v)
        else:
            dest[k] = v


class RpcUpdateType(Enum):
    """RPC Update type."""

    EVENT = auto()
    STATUS = auto()
    INITIALIZED = auto()
    DISCONNECTED = auto()
    UNKNOWN = auto()
    ONLINE = auto()


class RpcDevice:
    """refoss RPC device representation."""

    def __init__(
        self,
        aiohttp_session: ClientSession,
        options: ConnectionOptions,
    ) -> None:
        """Device init."""
        self.aiohttp_session: ClientSession = aiohttp_session
        self.options: ConnectionOptions = options
        self._refoss: dict[str, Any] | None = None
        self._status: dict[str, Any] | None = None
        self._event: dict[str, Any] | None = None
        self._config: dict[str, Any] | None = None
        self._wsrpc = WsRPC(
            options.ip_address, self._on_notification, port=options.port
        )

        self._update_listener: Callable | None = None
        self._initialize_lock = asyncio.Lock()
        self.initialized: bool = False
        self._initializing: bool = False
        self._last_error: RefossError | None = None

    @classmethod
    async def create(
        cls: type[RpcDevice],
        aiohttp_session: ClientSession,
        ip_or_options: IpOrOptionsType,
    ) -> RpcDevice:
        """Device creation."""
        options = await process_ip_or_options(ip_or_options)
        _LOGGER.debug(
            "host %s:%s: RPC device create, MAC: %s",
            options.ip_address,
            options.port,
            options.device_mac,
        )
        return cls(aiohttp_session, options)

    def _on_notification(
        self, source: RPCSource, method: str, params: dict[str, Any] | None = None
    ) -> None:
        """Received status notification from device."""
        if not self._update_listener:
            return

        update_type = RpcUpdateType.UNKNOWN
        if params is not None:
            if method == "NotifyStatus" and self._status is not None:
                mergedicts(self._status, params)
                update_type = RpcUpdateType.STATUS
            elif method == "NotifyEvent":
                self._event = params
                update_type = RpcUpdateType.EVENT
        elif method == NOTIFY_WS_CLOSED:
            update_type = RpcUpdateType.DISCONNECTED

        # inform listener that device is online
        if not self.initialized and not self._initializing:
            self._update_listener(self, RpcUpdateType.ONLINE)
            return

        # If the device isn't initialized, avoid sending updates
        # as it may be in the process of initializing.
        if self.initialized:
            self._update_listener(self, update_type)

    @property
    def ip_address(self) -> str:
        """Device ip address."""
        return self.options.ip_address

    @property
    def port(self) -> int:
        """Device port."""
        return self.options.port

    async def initialize(self) -> None:
        """Device initialization."""
        _LOGGER.debug("host %s:%s: RPC device initialize", self.ip_address, self.port)
        if self._initialize_lock.locked():
            raise RuntimeError("Already initializing")

        async with self._initialize_lock:
            self._initializing = True
            # First initialize may already have status from wakeup event
            # If device is initialized again we need to fetch new status
            if self.initialized:
                self.initialized = False
                self._status = None

            try:
                await self._connect_websocket()
            finally:
                self._initializing = False
                if self._update_listener and self.initialized:
                    self._update_listener(self, RpcUpdateType.INITIALIZED)

    async def _connect_websocket(self) -> None:
        """Connect device websocket."""
        ip = self.options.ip_address
        port = self.options.port
        try:
            async with asyncio.timeout(DEVICE_IO_TIMEOUT):
                await self._wsrpc.connect(self.aiohttp_session)
            await self._init_calls()
        except InvalidAuthError as err:
            self._last_error = InvalidAuthError(err)
            _LOGGER.debug("host %s:%s: error: %r", ip, port, self._last_error)
            await self._wsrpc.disconnect()
            raise
        except MacAddressMismatchError as err:
            self._last_error = err
            _LOGGER.debug("host %s:%s: error: %r", ip, port, err)
            await self._wsrpc.disconnect()
            raise
        except (*CONNECT_ERRORS, RpcCallError) as err:
            self._last_error = DeviceConnectionError(err)
            _LOGGER.debug("host %s:%s: error: %r", ip, port, self._last_error)
            await self._wsrpc.disconnect()
            raise self._last_error from err
        else:
            _LOGGER.debug("host %s:%s: RPC device init finished", ip, port)
            self.initialized = True
        _LOGGER.debug("device %s info:%s,", self.name, self.refoss)
        _LOGGER.debug("device %s status:%s,", self.name, self.status)
        _LOGGER.debug("device %s config:%s,", self.name, self.config)

    async def shutdown(self) -> None:
        """Shutdown device and remove the listener.

        This method will unsubscribe the update listener and disconnect the websocket.

        """
        _LOGGER.debug("host %s:%s: RPC device shutdown", self.ip_address, self.port)
        self._update_listener = None
        await self._wsrpc.disconnect()

    def subscribe_updates(self, update_listener: Callable) -> None:
        """Subscribe to device status updates."""
        self._update_listener = update_listener

    async def trigger_firmware_update(self) -> None:
        """Trigger an ota update."""
        await self.call_rpc("Refoss.Upgrade")

    async def trigger_check_latest_firmware(self) -> None:
        """Trigger a device check latest firmware."""
        await self.call_rpc("Refoss.Upgrade.Check")

    async def trigger_reboot(self, delay_ms: int = 1000) -> None:
        """Trigger a device reboot."""
        await self.call_rpc("Refoss.Device.Reboot")

    async def update_status(self) -> None:
        """Get device status from 'refoss.GetStatus'."""
        self._status = await self.call_rpc("Refoss.Status.Get")

    async def update_config(self) -> None:
        """Get device config from 'refoss.GetConfig'."""
        self._config = await self.call_rpc("Refoss.Config.Get")

    async def poll(self) -> None:
        """Poll device for calls that do not receive push updates."""
        calls: list[tuple[str, dict[str, Any] | None]] = [("Refoss.Status.Get", None)]

        results = await self.call_rpc_multiple(calls, DEVICE_POLL_TIMEOUT)
        self._status = results[0]

    async def _init_calls(self) -> None:
        """Make calls needed to initialize the device."""
        # refoss.GetDeviceInfo is the only RPC call that does not
        # require auth, so we must do a separate call here to get
        # the auth_domain/id so we can enable auth for the rest of the calls
        self._refoss = await self.call_rpc("Refoss.DeviceInfo.Get")
        if self.options.username and self.options.password:
            self._wsrpc.set_auth_data(
                self.dev_id,
                self.options.username,
                self.options.password,
            )

        mac = self.mac
        device_mac = self.options.device_mac
        if device_mac and device_mac != mac:
            raise MacAddressMismatchError(f"Input MAC: {device_mac}, refoss MAC: {mac}")

        calls: list[tuple[str, dict[str, Any] | None]] = [("Refoss.Config.Get", None)]
        if fetch_status := self._status is None:
            calls.append(("Refoss.Status.Get", None))

        results = await self.call_rpc_multiple(calls, DEVICE_INIT_TIMEOUT)

        self._config = results.pop(0)
        if fetch_status:
            self._status = results.pop(0)

    @property
    def requires_auth(self) -> bool:
        """Device check for authentication."""
        if "auth_en" not in self.refoss:
            raise WrongRefoss

        return bool(self.refoss["auth_en"])

    async def call_rpc(
        self, method: str, params: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        """Call RPC method."""
        return (await self.call_rpc_multiple(((method, params),)))[0]

    async def call_rpc_multiple(
        self,
        calls: Iterable[tuple[str, dict[str, Any] | None]],
        timeout: float = DEVICE_IO_TIMEOUT,
    ) -> list[dict[str, Any]]:
        """Call RPC method."""
        try:
            return await self._wsrpc.calls(calls, timeout)
        except (InvalidAuthError, RpcCallError) as err:
            self._last_error = err
            raise
        except CONNECT_ERRORS as err:
            self._last_error = DeviceConnectionError(err)
            raise DeviceConnectionError from err

    @property
    def status(self) -> dict[str, Any]:
        """Get device status."""
        if not self.initialized:
            raise NotInitialized

        if self._status is None:
            raise InvalidAuthError

        return self._status

    @property
    def event(self) -> dict[str, Any] | None:
        """Get device event."""
        if not self.initialized:
            raise NotInitialized

        return self._event

    @property
    def config(self) -> dict[str, Any]:
        """Get device config."""
        if not self.initialized:
            raise NotInitialized

        if self._config is None:
            raise InvalidAuthError

        return self._config

    @property
    def refoss(self) -> dict[str, Any]:
        """Device info."""
        if self._refoss is None:
            raise NotInitialized

        return self._refoss

    @property
    def dev_id(self) -> str:
        """Device uuid."""
        return cast(str, self.refoss["dev_id"])

    @property
    def mac(self) -> str:
        """Device mac."""
        return cast(str, self.refoss["mac"])

    @property
    def firmware_version(self) -> str:
        """Device firmware version."""
        return cast(str, self.refoss["fw_ver"])

    @property
    def hw_version(self) -> str:
        """Device version."""
        return cast(str, self.refoss["hw_ver"])

    @property
    def model(self) -> str:
        """Device model."""
        return cast(str, self.refoss["model"])

    @property
    def hostname(self) -> str:
        """Device hostname."""
        return cast(str, self.refoss["dev_id"])

    @property
    def name(self) -> str:
        """Device name."""
        return cast(str, self.refoss.get("name") or self.hostname)

    @property
    def connected(self) -> bool:
        """Return true if device is connected."""
        return self._wsrpc.connected

    @property
    def last_error(self) -> RefossError | None:
        """Return the last error during async device init."""
        return self._last_error
