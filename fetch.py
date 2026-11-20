"""Fetches robots.txt, /.well-known/tdmrep.json and /ai.txt of many hosts, politely.

Per host the requests are sequential (at most one at a time): robots.txt over https, over http only when https gave
no HTTP response (and the name resolved); then tdmrep.json and ai.txt on the scheme that answered, unless the host is
unreachable. Redirects are followed up to 5 hops (RFC 9309 2.3.1.2), also to other hosts. Certificates are not
verified: the files are public and only read. At most `per_domain` hosts of one registered domain are fetched at the
same time, so that a hosting platform with many subdomains sees a few requests at a time from one process.
With robots_only (attempt 3) only robots.txt is asked, and each connection is closed after its response.
"""

from __future__ import annotations

import asyncio
import socket
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

import aiohttp

import rules

LIMITS = {"robots": 512 * 1024, "tdmrep": 64 * 1024, "aitxt": 64 * 1024}
PATHS = {"robots": "/robots.txt", "tdmrep": "/.well-known/tdmrep.json", "aitxt": "/ai.txt"}


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def error_class(e: BaseException) -> str:
    if isinstance(e, aiohttp.TooManyRedirects):
        return "redirects"
    if isinstance(e, aiohttp.ClientConnectorDNSError) or (
        isinstance(e, aiohttp.ClientConnectorError) and isinstance(getattr(e, "os_error", None), socket.gaierror)
    ):
        return "dns"
    if isinstance(e, (aiohttp.ClientSSLError, aiohttp.ClientConnectorCertificateError)):
        return "tls"
    if isinstance(e, (asyncio.TimeoutError, aiohttp.ServerTimeoutError)):
        return "timeout"
    if isinstance(e, aiohttp.ClientConnectorError):
        return "connect"
    if isinstance(
        e,
        (
            aiohttp.ServerDisconnectedError,
            aiohttp.ClientPayloadError,
            aiohttp.ClientResponseError,
            aiohttp.ClientOSError,
        ),
    ):
        return "protocol"
    if isinstance(e, (aiohttp.InvalidURL, ValueError, UnicodeError)):
        return "url"
    return "other:" + type(e).__name__


def retry_after(value: str | None) -> float:
    """Seconds to wait after a 429: the Retry-After header (seconds or an HTTP date), else 10; between 1 and 60."""
    wait = 10.0
    if value:
        try:
            wait = float(value)
        except ValueError:
            try:
                wait = (parsedate_to_datetime(value) - datetime.now(timezone.utc)).total_seconds()
            except (TypeError, ValueError):
                wait = 10.0
    return min(60.0, max(1.0, wait))


async def get(session: aiohttp.ClientSession, url: str, limit: int, retries_429: int = 2) -> dict:
    """One GET: final status and URL, the body up to `limit` bytes, or the error class. A 429 (too many requests:
    our pace, not an opt-out) is waited out as its Retry-After says and asked again, up to `retries_429` times."""
    t0 = time.monotonic()
    for k in range(retries_429):
        res = await _get(session, url, limit, t0)
        if res["status"] != 429:
            res["r429"] = k
            return res
        await asyncio.sleep(res.pop("wait"))
    res = await _get(session, url, limit, t0)
    res.pop("wait", None)
    res["r429"] = retries_429
    return res


async def _get(session: aiohttp.ClientSession, url: str, limit: int, t0: float) -> dict:
    try:
        async with session.get(url, allow_redirects=True, max_redirects=5) as resp:
            body = bytearray()
            truncated = False
            while len(body) <= limit:
                chunk = await resp.content.read(65536)
                if not chunk:
                    break
                body += chunk
            if len(body) > limit:
                body, truncated = body[:limit], True
            res = {
                "status": resp.status,
                "url": str(resp.url),
                "body": bytes(body),
                "truncated": truncated,
                "error": None,
                "ms": int(1000 * (time.monotonic() - t0)),
            }
            if resp.status == 429:
                res["wait"] = retry_after(resp.headers.get("Retry-After"))
            return res
    except (aiohttp.ClientError, asyncio.TimeoutError, OSError, ValueError) as e:
        # a network or protocol failure is an outcome of the host; anything else is a bug and stops the process
        return {
            "status": None,
            "url": url,
            "body": None,
            "truncated": False,
            "error": error_class(e),
            "detail": f"{type(e).__name__}: {e}"[:300],
            "ms": int(1000 * (time.monotonic() - t0)),
        }


async def fetch_host(session: aiohttp.ClientSession, host: str, robots_only: bool = False) -> dict:
    rec: dict = {"host": host, "fetched_at": now_iso()}
    try:
        host.encode("ascii")
        if not host or len(host) > 253:
            raise ValueError("bad host")
    except (UnicodeError, ValueError):
        rec.update(
            robots_status=None,
            robots_error="url",
            robots_detail="host not fetchable",
            robots_url=None,
            robots_body=None,
            robots_truncated=False,
            robots_ms=0,
            scheme=None,
        )
        return rec
    res = None
    scheme = None
    for scheme in ("https", "http"):
        res = await get(session, f"{scheme}://{host}{PATHS['robots']}", LIMITS["robots"])
        if res["status"] is not None or res["error"] == "dns":
            break
    rec["r429"] = res["r429"]
    rec.update(
        scheme=scheme,
        robots_status=res["status"],
        robots_error=res["error"],
        robots_detail=res.get("detail"),
        robots_url=res["url"],
        robots_body=res["body"],
        robots_truncated=res["truncated"],
        robots_ms=res["ms"],
    )
    if robots_only or rules.robots_outcome(res["status"], res["error"]) == rules.UNREACHABLE:
        return rec
    for kind in ("tdmrep", "aitxt"):
        r = await get(session, f"{scheme}://{host}{PATHS[kind]}", LIMITS[kind])
        rec["r429"] += r["r429"]
        rec.update(
            {
                f"{kind}_status": r["status"],
                f"{kind}_error": r["error"],
                f"{kind}_body": r["body"] if r["status"] is not None and 200 <= r["status"] < 300 else None,
            }
        )
    return rec


class Fetcher:
    """One session, one limit of `concurrency` hosts at a time and one state per registered domain (at most
    `per_domain` hosts at a time, and the pause after a 429), shared by every batch of hosts fetched through it, also
    by batches that run at the same time."""

    def __init__(self, concurrency: int, per_domain: int, timeout: float = 30.0, robots_only: bool = False):
        self.concurrency, self.per_domain = concurrency, per_domain
        self.timeout, self.robots_only = timeout, robots_only
        self.domains: dict[str, dict] = {}

    async def __aenter__(self) -> Fetcher:
        # robots_only asks one file per host: a kept-alive connection would only stay open idle
        keep = {"force_close": True} if self.robots_only else {"keepalive_timeout": 5}
        connector = aiohttp.TCPConnector(
            limit=0,
            limit_per_host=2,
            ttl_dns_cache=600,
            family=socket.AF_INET,
            ssl=False,
            resolver=aiohttp.AsyncResolver(timeout=5.0, tries=2),
            enable_cleanup_closed=True,
            **keep,
        )
        tmo = aiohttp.ClientTimeout(total=self.timeout, sock_connect=10, sock_read=20)
        headers = {"User-Agent": rules.USER_AGENT, "Accept": "text/plain, application/json, */*;q=0.5"}
        self.gate = asyncio.Semaphore(self.concurrency)
        self.session = aiohttp.ClientSession(
            connector=connector,
            timeout=tmo,
            headers=headers,
            max_line_size=65536,
            max_field_size=65536,
            cookie_jar=aiohttp.DummyCookieJar(),
        )
        return self

    async def __aexit__(self, *exc):
        await self.session.close()

    async def fetch(self, hosts: list[tuple[str, str]], progress=None) -> list[dict]:
        """Fetches every (host, registered domain); returns one record per host, in input order."""
        out: list[dict | None] = [None] * len(hosts)
        done = 0

        async def one(i: int, host: str, regdom: str):
            nonlocal done
            st = self.domains.setdefault(
                regdom or host, {"sem": asyncio.Semaphore(self.per_domain), "slow": asyncio.Lock(), "delay": 0.0}
            )
            async with st["sem"]:
                if st["delay"] > 0:  # this domain answered 429: one host at a time, with a pause
                    async with st["slow"]:
                        await asyncio.sleep(st["delay"])
                        async with self.gate:
                            rec = await fetch_host(self.session, host, self.robots_only)
                else:
                    async with self.gate:
                        rec = await fetch_host(self.session, host, self.robots_only)
            if rec.get("r429") or 429 in (rec.get("robots_status"), rec.get("tdmrep_status"), rec.get("aitxt_status")):
                st["delay"] = min(8.0, max(1.0, 2 * st["delay"]))
            else:
                st["delay"] = st["delay"] / 2 if st["delay"] > 0.25 else 0.0
            out[i] = rec
            done += 1
            if progress and done % 1000 == 0:
                progress(done)

        await asyncio.gather(*(one(i, h, d) for i, (h, d) in enumerate(hosts)))
        return out  # type: ignore[return-value]


async def fetch_all(
    hosts: list[tuple[str, str]],
    concurrency: int,
    per_domain: int,
    timeout: float = 30.0,
    progress=None,
    robots_only: bool = False,
) -> list[dict]:
    """Fetches every (host, registered domain); returns one record per host, in input order."""
    async with Fetcher(concurrency, per_domain, timeout, robots_only) as f:
        return await f.fetch(hosts, progress)
