"""HTTP client for calling other services on the platform."""

from typing import Any

import httpx


class ServiceClient:
    """Calls another service from its base URL.

    It is created once per dependency (for example once when the service
    starts, or in the constructor of an injected client) and is reused for
    every request — a new instance is not created per request. It keeps an
    `httpx.Client` open for its whole life with its connection pool, which is
    the right thing for a long-lived service as long as it is not rebuilt on
    every call.
    """

    def __init__(self, base_url: str, timeout: float = 5.0) -> None:
        self._client = httpx.Client(base_url=base_url, timeout=timeout)

    def get_json(self, path: str) -> Any:
        res = self._client.get(path)
        res.raise_for_status()
        return res.json()

    def post_json(self, path: str, body: Any) -> Any:
        res = self._client.post(path, json=body)
        res.raise_for_status()
        return res.json()
