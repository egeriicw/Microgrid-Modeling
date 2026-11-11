"""Server-side authentication and role checks (FR-WEB-5).

Decision: bearer API keys configured per subject and role in ``MICROGRID_API_KEYS``. The key is compared in constant
time; the role comes from the configuration, never from the request. Permission flags (e.g. ``filters.use``) are asked
through ``microgrid.community_load.permissions.can``; role names are compared only where a rule is about the role
itself (admin sees all runs; demo uses only predefined compositions).
"""

from __future__ import annotations

import hmac
from dataclasses import dataclass

from fastapi import Depends, HTTPException, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

_bearer = HTTPBearer(auto_error=False, description="API key issued per user and role (MICROGRID_API_KEYS).")


@dataclass(frozen=True)
class Principal:
    subject: str
    role: str


def current_principal(request: Request, creds: HTTPAuthorizationCredentials | None = Depends(_bearer)) -> Principal:
    unauthorized = HTTPException(
        status_code=401, detail="authentication required", headers={"WWW-Authenticate": "Bearer"}
    )
    if creds is None or creds.scheme.lower() != "bearer":
        raise unauthorized
    presented = creds.credentials.encode()
    found: Principal | None = None
    for k in request.app.state.settings.api_keys:  # compare against every key: no early exit on a match
        if hmac.compare_digest(presented, k.key.get_secret_value().encode()):
            found = Principal(subject=k.subject, role=k.role)
    if found is None:
        raise unauthorized
    return found
