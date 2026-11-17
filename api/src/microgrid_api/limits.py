"""Request body size cap (FR-PLAT-6 "request size limits").

A pure ASGI middleware: a declared ``Content-Length`` above the cap is refused with 413 before the application runs;
a body without one (chunked upload) is counted as it arrives and refused as soon as it passes the cap, so an oversized
body is never fully buffered or parsed. A malformed ``Content-Length`` is a 400. Response bodies never echo the
request.
"""

from __future__ import annotations

import json
from typing import Any

from fastapi import HTTPException
from starlette.types import ASGIApp, Message, Receive, Scope, Send


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

        async def limited() -> Message:
            nonlocal received
            message: dict[str, Any] = dict(await receive())
            if message["type"] == "http.request":
                received += len(message.get("body", b""))
                if received > self.max_bytes:
                    # FastAPI re-raises HTTPException from body reading, so this becomes a clean 413 response.
                    raise HTTPException(413, too_large)
            return message

        await self.app(scope, limited, send)
