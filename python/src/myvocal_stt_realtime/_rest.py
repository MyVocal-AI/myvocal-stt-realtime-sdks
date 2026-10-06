"""REST transport: envelope unwrapping, idempotent create, lifecycle helpers."""
import asyncio
import json
import logging
import time
import uuid
from typing import Any, Dict, Optional

import httpx

from ._version import __version__
from .errors import ApiError, TransportError, api_error_from_envelope
from .models import SessionRequest, SessionView, Ticket

REALTIME_PATH = "/sound_clone/api/v1/stt/realtime"
USER_AGENT = "myvocal-stt-realtime-python/%s" % __version__
log = logging.getLogger("myvocal_stt_realtime")


class RestTransport:
    def __init__(self, access_key: str, config, *, http_client: Optional[httpx.AsyncClient] = None,
                 proxy: Optional[str] = None, trust_env: bool = True, verify: Any = True):
        if not access_key or not isinstance(access_key, str):
            raise ValueError("access_key is required")
        self._key = access_key
        self._config = config
        self._owned = http_client is None
        if http_client is None:
            kwargs: Dict[str, Any] = dict(timeout=config.timeouts.request, trust_env=trust_env, verify=verify,
                                          headers={"User-Agent": USER_AGENT})
            if proxy:
                kwargs["proxy"] = proxy
            http_client = httpx.AsyncClient(**kwargs)
        self._http = http_client

    def __repr__(self) -> str:  # never shows the key
        return "RestTransport(base_url=%r)" % self._config.base_url

    @property
    def base_url(self) -> str:
        return self._config.base_url

    async def aclose(self) -> None:
        if self._owned:
            await self._http.aclose()

    def _headers(self, extra: Optional[Dict[str, str]] = None) -> Dict[str, str]:
        h = {"accessKey": self._key, "Content-Type": "application/json", "Accept": "application/json",
             "User-Agent": USER_AGENT}
        if extra:
            h.update(extra)
        return h

    async def call(self, method: str, path: str, body: Optional[Dict[str, Any]] = None, *,
                   headers: Optional[Dict[str, str]] = None, timeout: Optional[float] = None,
                   deadline: Optional[float] = None) -> Dict[str, Any]:
        """One REST request. ``deadline`` (a ``time.monotonic()`` value) is an overall budget
        owned by the caller, e.g. one close-out: the request gets only the time that is left
        (never more than ``timeout``) and is bounded as a whole, not per connect/read phase."""
        url = self.base_url + REALTIME_PATH + path
        op = "%s %s" % (method, REALTIME_PATH + path.split("?")[0])
        limit = timeout or self._config.timeouts.request
        if deadline is not None:
            left = deadline - time.monotonic()
            if left <= 0:
                raise TransportError("request not sent, the time budget is used up: " + op)
            limit = min(limit, left)
        request = self._http.request(method, url, headers=self._headers(headers),
                                     content=None if body is None else json.dumps(body), timeout=limit)
        try:
            resp = await (request if deadline is None else asyncio.wait_for(request, limit))
        except (httpx.TimeoutException, asyncio.TimeoutError) as exc:
            raise TransportError("request timed out: " + op) from exc
        except httpx.HTTPError as exc:
            raise TransportError("request failed: %s %s (%s)" % (method, path, type(exc).__name__)) from exc
        try:
            payload = resp.json()
        except ValueError:
            raise TransportError("HTTP %d without a MyVocal envelope" % resp.status_code, status=resp.status_code)
        if not isinstance(payload, dict) or "code" not in payload:
            raise TransportError("HTTP %d without a MyVocal envelope" % resp.status_code, status=resp.status_code)
        if payload.get("code") != 1:
            raise api_error_from_envelope(payload)
        if resp.status_code >= 400:
            raise TransportError("HTTP %d" % resp.status_code, status=resp.status_code)
        data = payload.get("data")
        return data if isinstance(data, dict) else {}

    async def create(self, request: SessionRequest, idempotency_key: Optional[str]) -> SessionView:
        key = idempotency_key or str(uuid.uuid4())
        body = request.to_wire()  # the same body for every retry
        attempts = 1 + max(0, self._config.max_create_retries)
        deadline = time.monotonic() + self._config.timeouts.create
        last: Optional[BaseException] = None
        for attempt in range(attempts):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            try:
                data = await self.call("POST", "/sessions", body, headers={"Idempotency-Key": key},
                                       timeout=min(remaining, self._config.timeouts.request))
                return SessionView.from_wire(data)
            except ApiError as err:
                if not err.retryable:
                    raise
                last = err
            except TransportError as err:
                if err.status is not None and err.status < 500 and err.status != 429:
                    raise
                last = err
            log.debug("create attempt %d failed (%s); retrying with the same Idempotency-Key",
                      attempt + 1, type(last).__name__)
            if attempt + 1 < attempts:
                await asyncio.sleep(min(0.25 * (2 ** attempt), 2.0, max(0.0, deadline - time.monotonic())))
        assert last is not None
        raise last

    async def get(self, session_id: str, *, deadline: Optional[float] = None) -> SessionView:
        return SessionView.from_wire(await self.call("GET", "/sessions/" + _sid(session_id), deadline=deadline))

    async def pause(self, session_id: str, captured: Optional[int] = None) -> SessionView:
        return SessionView.from_wire(await self.call("POST", "/sessions/%s/pause" % _sid(session_id), _cap(captured)))

    async def resume(self, session_id: str, captured: Optional[int] = None) -> SessionView:
        return SessionView.from_wire(await self.call("POST", "/sessions/%s/resume" % _sid(session_id), _cap(captured)))

    async def finish(self, session_id: str, captured: Optional[int] = None,
                     timeout: Optional[float] = None, *, deadline: Optional[float] = None) -> SessionView:
        return SessionView.from_wire(await self.call("POST", "/sessions/%s/finish" % _sid(session_id),
                                                     _cap(captured), timeout=timeout or max(
                                                         self._config.timeouts.request, 70.0), deadline=deadline))

    async def ticket(self, session_id: str) -> Ticket:
        return Ticket.from_wire(await self.call("POST", "/sessions/%s/tickets" % _sid(session_id), {}))


def _cap(captured: Optional[int]) -> Dict[str, Any]:
    return {} if captured is None else {"capturedSamples": str(int(captured))}


def _sid(session_id: str) -> str:
    if not session_id or "/" in session_id or "?" in session_id:
        raise ValueError("invalid session id")
    return session_id
