"""WsRpc for Refoss."""

from __future__ import annotations

import asyncio
import random
import string
import contextlib
import hashlib
import logging
import socket
import time
from asyncio import Task, tasks
from collections.abc import Callable, Coroutine, Iterable
from dataclasses import dataclass
from enum import Enum, auto
from http import HTTPStatus
from typing import TYPE_CHECKING, Any

from aiohttp import (
    ClientSession,
    ClientWebSocketResponse,
    WSMessage,
    WSMsgType,
    client_exceptions,
)
from yarl import URL

from ..const import (
    DEFAULT_HTTP_PORT,
    NOTIFY_WS_CLOSED,
    UNDEFINED,
    WS_HEARTBEAT,
    UndefinedType,
)
from ..exceptions import (
    ConnectionClosed,
    DeviceConnectionError,
    DeviceConnectionTimeoutError,
    InvalidAuthError,
    InvalidMessage,
    RpcCallError,
)
from ..json import json_bytes, json_loads

_LOGGER = logging.getLogger(__name__)

BUFFER_SIZE = 1024 * 64


class RPCSource(Enum):
    """RPC message source."""

    CLIENT = auto()
    SERVER = auto()


def _receive_json_or_raise(msg: WSMessage) -> dict[str, Any]:
    """Receive json or raise."""
    if msg.type is WSMsgType.TEXT:
        try:
            data: dict[str, Any] = json_loads(msg.data)
        except ValueError as err:
            _LOGGER.debug(
                "JSON parse error: data=%s, error=%s",
                msg.data[:200] if msg.data else None,
                err,
            )
            raise InvalidMessage(f"Received invalid JSON: {msg.data}") from err
        return data

    if msg.type in (WSMsgType.CLOSE, WSMsgType.CLOSED, WSMsgType.CLOSING):
        raise ConnectionClosed("Connection was closed.")

    if msg.type is WSMsgType.ERROR:
        _LOGGER.error("WebSocket error message received")
        raise InvalidMessage("Received message error")

    _LOGGER.warning("Unexpected WebSocket message type: %s", msg.type)
    raise InvalidMessage(f"Received non-Text message: {msg.type}")


def hex_hash(message: str) -> str:
    """Get hex representation of sha256 hash of string."""
    return hashlib.sha256(message.encode("utf-8")).hexdigest()


HA2 = hex_hash("dummy_method:dummy_uri")


@dataclass
class AuthData:
    """RPC Auth data class."""

    realm: str
    username: str
    password: str

    def __post_init__(self) -> None:
        """Call after initialization."""
        self.ha1 = hex_hash(f"{self.username}:{self.realm}:{self.password}")

    def get_auth(self, nonce: int | None = None, n_c: int = 1) -> dict[str, Any]:
        """Get auth for RPC calls."""
        cnonce = "".join(
            random.SystemRandom().choice(string.ascii_uppercase + string.digits)
            for _ in range(16)
        )
        if nonce is None:
            nonce = int(time.time())
        hashed = hex_hash(f"{self.ha1}:{nonce}:{n_c}:{cnonce}:auth:{HA2}")

        return {
            "realm": self.realm,
            "username": self.username,
            "nonce": nonce,
            "cnonce": cnonce,
            "response": hashed,
            "algorithm": "SHA-256",
        }


@dataclass
class SessionData:
    """SessionData (src/dst/auth) class."""

    src: str | None
    dst: str | None
    auth: dict[str, Any] | None


class RPCCall:
    """RPCCall class."""

    __slots__ = (
        "auth",
        "call_id",
        "dst",
        "method",
        "params",
        "resolve",
        "result",
        "src",
    )

    def __init__(
        self,
        call_id: int,
        method: str,
        params: dict[str, Any] | None,
        session: SessionData,
        resolve: asyncio.Future[dict[str, Any]],
    ) -> None:
        """Initialize RPC class."""
        self.auth = session.auth
        self.call_id = call_id
        self.params = params
        self.method = method
        self.src = session.src
        self.dst = session.dst
        self.resolve = resolve
        self.result: dict[str, Any] | UndefinedType = UNDEFINED

    def __repr__(self) -> str:
        """Return representation of the call."""
        return (
            f"<RPCCall method={self.method!r} params={self.params!r} "
            f"call_id={self.call_id} result={self.result!r}>"
        )

    def __repr__(self) -> str:
        """Return representation of the call."""
        return (
            "<RPCCall "
            f"method={self.method} "
            f"params={self.params} "
            f"call_id={self.call_id} "
            f"result={self.result}"
            ">"
        )

    @property
    def request_frame(self) -> dict[str, Any]:
        """Request frame."""
        msg = {
            "id": self.call_id,
            "method": self.method,
            "src": self.src,
        }
        for obj in ("params", "dst", "auth"):
            if getattr(self, obj) is not None:
                msg[obj] = getattr(self, obj)

        return msg


class WsBase:
    """Base class for WebSocket handlers."""

    def __init__(self) -> None:
        """Initialize WsBase class."""
        self._background_tasks: set[Task] = set()

    def _create_and_track_task(self, func: Coroutine) -> None:
        """Create and and hold strong reference to the task."""
        task = asyncio.create_task(func)
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)


class WsRPC(WsBase):
    """WsRPC class."""

    def __init__(
        self,
        ip_address: str,
        on_notification: Callable[[RPCSource, str, dict | None], None],
        port: int = DEFAULT_HTTP_PORT,
    ) -> None:
        """Initialize WsRPC class."""
        super().__init__()
        self._auth_data: AuthData | None = None
        self._ip_address = ip_address
        self._port = port
        self._on_notification = on_notification
        self._rx_task: tasks.Task[None] | None = None
        self._client: ClientWebSocketResponse | None = None
        self._calls: dict[int, RPCCall] = {}
        self._call_id = 0
        self._session = SessionData(f"aios-{id(self)}", None, None)

    @property
    def _loop(self) -> asyncio.AbstractEventLoop:
        """Get current running event loop."""
        return asyncio.get_running_loop()

    @property
    def _next_id(self) -> int:
        self._call_id += 1
        return self._call_id

    async def connect(self, aiohttp_session: ClientSession) -> None:
        """Connect to device."""
        if self.connected:
            _LOGGER.warning(
                "host %s:%s: Connection attempt while already connected",
                self._ip_address,
                self._port,
            )
            raise RuntimeError("Already connected")

        ws_url = URL.build(
            scheme="http", host=self._ip_address, port=self._port, path="/rpc"
        )
        try:
            self._client = await aiohttp_session.ws_connect(
                ws_url,
                heartbeat=WS_HEARTBEAT,
            )
        except client_exceptions.WSServerHandshakeError as err:
            _LOGGER.warning(
                "host %s:%s: WebSocket handshake failed: status=%s, message=%s",
                self._ip_address,
                self._port,
                getattr(err, "status", "unknown"),
                str(err)[:200],
            )
            raise DeviceConnectionError(err) from err
        except client_exceptions.ClientError as err:
            _LOGGER.warning(
                "host %s:%s: WebSocket client error: %s",
                self._ip_address,
                self._port,
                str(err)[:200],
            )
            raise DeviceConnectionError(err) from err

        sock: socket.socket = self._client.get_extra_info("socket")
        try:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, BUFFER_SIZE)
        except OSError as err:
            _LOGGER.warning(
                "host %s:%s: Failed to set socket receive buffer size: %s",
                self._ip_address,
                self._port,
                err,
            )

        self._rx_task = asyncio.create_task(self._rx_msgs())

        _LOGGER.info("Connected to %s", self._ip_address)

    async def disconnect(self) -> None:
        """Disconnect all sessions."""
        if self._rx_task is not None:
            self._rx_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._rx_task
            self._rx_task = None
        if self._client is None:
            return

        await self._client.close()

    def set_auth_data(self, realm: str, username: str, password: str) -> None:
        """Set authentication data and generate session auth."""
        self._auth_data = AuthData(realm, username, password)
        self._session.auth = self._auth_data.get_auth()

    async def _handle_call(self, frame_id: str) -> None:
        if TYPE_CHECKING:
            assert self._client

        await self._send_json(
            {
                "id": frame_id,
                "src": self._session.src,
                "error": {"code": 500, "message": "Not Implemented"},
            }
        )

    def handle_frame(self, source: RPCSource, frame: dict[str, Any]) -> None:
        """Handle RPC frame."""

        if peer_src := frame.get("src"):
            if self._session.dst is not None and peer_src != self._session.dst:
                _LOGGER.warning(
                    "Remote src changed: %s -> %s", self._session.dst, peer_src
                )
            self._session.dst = peer_src

        frame_id = frame.get("id")

        if method := frame.get("method"):
            # peer is invoking a method
            params = frame.get("params")
            if frame_id:
                # and expects a response
                _LOGGER.debug("handle call for frame_id: %s", frame_id)
                self._create_and_track_task(self._handle_call(frame_id))
            else:
                # this is a notification
                _LOGGER.debug("Notification: %s %s", method, params)
                self._on_notification(source, method, params)

            return

        if frame_id:
            # looks like a response
            if (call := self._calls.pop(frame_id, None)) is None:
                _LOGGER.warning(
                    "Response from (%s:%s) for an unknown request id: %s, frame=%s",
                    self._ip_address,
                    self._port,
                    frame_id,
                    frame,
                )
                return

            if not call.resolve.cancelled():
                call.resolve.set_result(frame)

            return

        _LOGGER.warning(
            "Invalid frame format from (%s:%s): %s",
            self._ip_address,
            self._port,
            frame,
        )

    async def _rx_msgs(self) -> None:
        if TYPE_CHECKING:
            assert self._client

        try:
            while True:
                msg = await self._client.receive()
                try:
                    frame = _receive_json_or_raise(msg)
                    _LOGGER.debug(
                        "recv(%s:%s): %s", self._ip_address, self._port, frame
                    )
                except InvalidMessage as err:
                    _LOGGER.error(
                        "Invalid Message from host %s:%s: %s",
                        self._ip_address,
                        self._port,
                        err,
                    )
                    continue  # Skip to next message on invalid frame
                except ConnectionClosed:
                    break
                except Exception:
                    _LOGGER.exception("Unexpected error while receiving message")
                    raise

                if self._client.closed:
                    break

                self.handle_frame(RPCSource.CLIENT, frame)
        finally:
            _LOGGER.debug(
                "Websocket client connection from %s:%s closed",
                self._ip_address,
                self._port,
            )

            for call_item in self._calls.values():
                if not call_item.resolve.done():
                    call_item.resolve.set_exception(DeviceConnectionError(call_item))
            self._calls.clear()

            client = self._client
            self._client = None
            self._on_notification(RPCSource.CLIENT, NOTIFY_WS_CLOSED, None)

            # Close last since the await can yield
            # to the event loop and we want to minimize
            # race conditions
            if not client.closed:
                await client.close()

    @property
    def connected(self) -> bool:
        """Return if we're currently connected."""
        return self._client is not None and not self._client.closed

    async def call(
        self,
        method: str,
        params: dict[str, Any] | None = None,
        timeout: int = 10,
    ) -> dict[str, Any]:
        """Websocket RPC call."""
        return (await self.calls([(method, params)], timeout))[0]

    def _raise_for_unrecoverable_errors(
        self, resp: dict[str, Any], allow_auth_retry: bool
    ) -> None:
        """Raise for unrecoverable errors."""
        try:
            error = resp["error"]
            code = error.get("code", 0)
            msg = error.get("message", str(error))
        except (KeyError, TypeError) as err:
            raise RpcCallError(0, f"bad response format: {resp}") from err

        if code != HTTPStatus.UNAUTHORIZED.value:
            raise RpcCallError(code, msg)

        if allow_auth_retry and self._auth_data is not None:
            return

        raise InvalidAuthError(msg)

    async def calls(
        self, calls: Iterable[tuple[str, dict[str, Any] | None]], timeout: float = 10.0
    ) -> list[dict[str, Any]]:
        """Websocket RPC calls."""
        # Try request with initial/last call auth data
        call_list = list(calls)
        all_successful, results = await self._rpc_calls(call_list, timeout)
        if all_successful:
            # If all_successful, return results immediately
            # mypy does not know that .result is never
            # None when all_successful is True so we need
            # to ignore the type check here
            return [call.result for call in results]  # type: ignore[misc]

        # Partial success, try to update auth and retry
        to_retry: list[RPCCall] = []
        successful: list[dict[str, Any]] = []
        for call in results:
            if (result := call.result) is not UNDEFINED:
                successful.append(result)
                continue
            resp = call.resolve.result()
            self._raise_for_unrecoverable_errors(resp, allow_auth_retry=True)
            if not to_retry:
                # Update auth from response and try with new auth data
                # If we have multiple calls, we only need to update auth once
                if self._auth_data is None:
                    # This should not happen as _raise_for_unrecoverable_errors checks it
                    # But we handle it gracefully just in case
                    _LOGGER.error(
                        "host %s:%s: Auth retry required but no auth data configured",
                        self._ip_address,
                        self._port,
                    )
                    raise InvalidAuthError("Authentication data not configured")
                try:
                    auth = json_loads(resp["error"]["message"])
                    nonce = auth["nonce"]
                    nc = auth.get("nc", 1)
                except (KeyError, ValueError) as err:
                    _LOGGER.error(
                        "host %s:%s: Failed to parse auth challenge from response: %s, error: %s",
                        self._ip_address,
                        self._port,
                        resp,
                        err,
                    )
                    raise RpcCallError(
                        HTTPStatus.UNAUTHORIZED.value, "Invalid auth challenge format"
                    ) from err
                self._session.auth = self._auth_data.get_auth(nonce, nc)
            to_retry.append(call)

        _, results = await self._rpc_calls(
            [(call.method, call.params) for call in to_retry], timeout
        )

        for call in results:
            if (result := call.result) is UNDEFINED:
                resp = call.resolve.result()
                _LOGGER.warning(
                    "host %s:%s: RPC call retry failed: method=%s, error=%s",
                    self._ip_address,
                    self._port,
                    call.method,
                    resp.get("error"),
                )
                self._raise_for_unrecoverable_errors(resp, allow_auth_retry=False)
            else:
                successful.append(result)

        return successful

    async def _rpc_calls(
        self, rpc_calls: Iterable[tuple[str, dict[str, Any] | None]], timeout: float
    ) -> tuple[bool, list[RPCCall]]:
        """Websocket RPC call."""
        if self._client is None:
            raise RuntimeError("Not connected")

        sent_calls: list[RPCCall] = []
        loop = self._loop
        all_successful: bool = True
        future: asyncio.Future[dict[str, Any]]

        try:
            async with asyncio.timeout(timeout):
                for method, params in rpc_calls:
                    call_id = self._next_id
                    future = loop.create_future()
                    call = RPCCall(call_id, method, params, self._session, future)
                    sent_calls.append(call)
                    self._calls[call_id] = call
                    await self._send_json(call.request_frame)

                # Wait for all the responses
                for call in sent_calls:
                    response = await call.resolve
                    if "result" not in response:
                        all_successful = False
                        continue
                    call.result = response["result"]
        except (TimeoutError, Exception) as exc:
            # Clean up on any exception (timeout, connection close, etc.)
            for call in sent_calls:
                with contextlib.suppress(asyncio.CancelledError, asyncio.InvalidStateError):
                    if not call.resolve.done():
                        call.resolve.cancel()
                        await call.resolve
                # Ensure the call is removed from the calls dict on failure
                self._calls.pop(call.call_id, None)
            if isinstance(exc, TimeoutError):
                raise DeviceConnectionTimeoutError(sent_calls) from exc
            raise  # Re-raise other exceptions (DeviceConnectionError, etc.)

        # Clean up calls dict on success path
        for call in sent_calls:
            self._calls.pop(call.call_id, None)

        if _LOGGER.isEnabledFor(logging.DEBUG):
            for call in sent_calls:
                _LOGGER.debug(
                    "result(%s:%s): %s(%s) -> %s",
                    self._ip_address,
                    self._port,
                    call.method,
                    call.params,
                    call.result,
                )

        return all_successful, sent_calls

    async def _send_json(self, data: dict[str, Any]) -> None:
        """Send json frame to device."""
        _LOGGER.debug("send(%s:%s): %s", self._ip_address, self._port, data)

        if self._client is None or self._client.closed:
            raise DeviceConnectionError("WebSocket connection is not available")

        try:
            await self._client.send_frame(json_bytes(data), WSMsgType.TEXT)
        except (ConnectionError, RuntimeError) as err:
            _LOGGER.debug(
                "Failed to send message to %s:%s: %s",
                self._ip_address,
                self._port,
                err,
            )
            raise DeviceConnectionError("Failed to send message") from err
