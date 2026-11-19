"""Request body size cap (FR-PLAT-6 "request size limits").

A pure ASGI middleware with no dependence on the framework inside it:
* a declared ``Content-Length`` above the cap is refused with 413 before the application runs (a malformed or repeated
  ``Content-Length`` is a 400);
* a body without one (chunked upload) is counted as it arrives. Past the cap the wrapped ``receive`` raises a private
  exception and the application gets no more body. The middleware catches that exception (or notices the overflow
  even if the application swallowed it) and sends 413 itself, but only if the application has not started a
  response; if it has, the middleware stops quietly (the client sees a truncated response, never a second one).
Response bodies never echo the request.
"""

from __future__ import annotations

import json
from typing import Any

from starlette.types import ASGIApp, Message, Receive, Scope, Send


class _BodyTooLarge(Exception):
    """Raised inside the wrapped ``receive``; never leaves this module."""


async def _reply(send: Send, status: int, detail: str) -> None:
    body = json.dumps({"detail": detail}).encode()
    await send(
        {
            "type": "http.response.start",
            "status": status,
            "headers": [(b"content-type", b"application/json"), (b"content-length", str(len(body)).encode())],
        }
    )
    await send({"type": "http.response.body", "body": body})


class BodySizeLimit:
    def __init__(self, app: ASGIApp, max_bytes: int) -> None:
        self.app = app
        self.max_bytes = max_bytes

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        too_large = f"request body exceeds the server limit of {self.max_bytes} bytes"
        declared = [v for k, v in scope.get("headers", []) if k.lower() == b"content-length"]
        if declared:
            try:
                length = int(declared[0])
                if length < 0 or len(declared) > 1:
                    raise ValueError
            except ValueError:
                await _reply(send, 400, "invalid Content-Length header")
                return
            if length > self.max_bytes:
                await _reply(send, 413, too_large)
                return

        received = 0
        exceeded = False
        started = False

        async def limited_receive() -> Message:
            nonlocal received, exceeded
            if exceeded:
                raise _BodyTooLarge
            message: dict[str, Any] = dict(await receive())
            if message["type"] == "http.request":
                received += len(message.get("body", b""))
                if received > self.max_bytes:
                    exceeded = True
                    raise _BodyTooLarge
            return message

        async def guarded_send(message: Message) -> None:
            nonlocal started
            if exceeded:
                return  # the 413 (or nothing, if a response had already started) is sent by the middleware
            if message["type"] == "http.response.start":
                started = True
            await send(message)

        try:
            await self.app(scope, limited_receive, guarded_send)
        except _BodyTooLarge:
            pass
        except Exception:
            if not exceeded:
                raise
            # the application turned our signal into some other error after the cap was passed: still a 413
        if exceeded and not started:
            await _reply(send, 413, too_large)
