"""`Cache-Control: no-store` on every API response (security hardening).

API responses carry per-user data (identity, runs, results) and must not be kept by a browser or a shared cache. A
pure ASGI middleware sets the header on every response under the API prefix, errors included (401, 404, 413, 422,
500 rendered by the framework), replacing any other Cache-Control value. Streaming responses (server-sent events)
are unaffected apart from the header: the body is passed through untouched.
"""

from __future__ import annotations

from starlette.types import ASGIApp, Message, Receive, Scope, Send


class NoStore:
    def __init__(self, app: ASGIApp, prefix: str) -> None:
        self.app = app
        self.prefix = prefix.rstrip("/")

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        path = scope.get("path", "")
        if scope["type"] != "http" or not (path == self.prefix or path.startswith(self.prefix + "/")):
            await self.app(scope, receive, send)
            return

        async def send_no_store(message: Message) -> None:
            if message["type"] == "http.response.start":
                headers = [(k, v) for k, v in message.get("headers", []) if k.lower() != b"cache-control"]
                headers.append((b"cache-control", b"no-store"))
                message = {**message, "headers": headers}
            await send(message)

        await self.app(scope, receive, send_no_store)
