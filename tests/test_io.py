import asyncio
import io
import random

import pyarrow as pa
import pyarrow.parquet as pq
from aiohttp import web

import fetch
import hfread
import rules


class LocalParquet(hfread.HFParquet):
    """HFParquet on bytes in memory, counting the bytes it reads."""

    def __init__(self, data: bytes):
        self.data = data
        self.read_bytes = 0
        super().__init__("x/y", "rev", "f.parquet", threads=2)

    def _resolve(self):
        self.url, self.size, self.etag, self.commit = "mem", len(self.data), "e", "c"

    def _get(self, start, end):
        self.read_bytes += end - start
        return self.data[start:end]


def test_hfparquet_reads_row_groups_and_columns():
    n = 10000
    rnd = random.Random(0)
    t = pa.table(
        {
            "text": ["%0800x" % rnd.getrandbits(3200) for i in range(n)],
            "dump": ["CC-MAIN-2020-05"] * n,
            "url": [f"https://h{i % 7}.nl/{i}" for i in range(n)],
        }
    )
    buf = io.BytesIO()
    pq.write_table(t, buf, row_group_size=1000, compression="zstd")
    f = LocalParquet(buf.getvalue())
    assert f.metadata.num_row_groups == 10 and len(buf.getvalue()) > 3 << 20
    before = f.read_bytes
    got = f.read([1, 3], columns=["dump", "url"])
    assert (
        got.column("url").to_pylist() == t.column("url").to_pylist()[1000:2000] + t.column("url").to_pylist()[3000:4000]
    )
    assert f.read_bytes - before < 100_000  # the text column was not read
    last = f.read([9], columns=["url"])  # inside the footer window of the file: overlapping segments
    assert last.column("url").to_pylist() == t.column("url").to_pylist()[9000:]
    both = f.read([8, 9], columns=["dump", "url"])
    assert both.num_rows == 2000
    full = f.read([4])
    assert full.equals(t.slice(4000, 1000))


async def _serve(handler):
    app = web.Application()
    app.router.add_route("GET", "/{tail:.*}", handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]
    return runner, f"127.0.0.1:{port}"


def test_fetch_outcomes():
    async def run():
        async def ok(req):
            if req.path == "/robots.txt":
                return web.Response(text="User-agent: GPTBot\nDisallow: /\n")
            if req.path == "/.well-known/tdmrep.json":
                return web.Response(text='[{"location": "/", "tdm-reservation": 1}]')
            return web.Response(status=404)

        async def missing(req):
            return web.Response(status=404)

        async def denied(req):
            return web.Response(status=403)

        async def broken(req):
            return web.Response(status=503)

        async def loop(req):
            raise web.HTTPFound("/robots.txt?again")

        async def slow(req):
            await asyncio.sleep(5)
            return web.Response(text="late")

        runners, hosts = [], {}
        for name, h in (
            ("ok", ok),
            ("missing", missing),
            ("denied", denied),
            ("broken", broken),
            ("loop", loop),
            ("slow", slow),
        ):
            r, host = await _serve(h)
            runners.append(r)
            hosts[name] = host
        seen_ua = []

        async def ua(req):
            seen_ua.append(req.headers.get("User-Agent"))
            return web.Response(status=404)

        r, hosts["ua"] = await _serve(ua)
        runners.append(r)
        recs = await fetch.fetch_all([(h, h) for h in hosts.values()], concurrency=8, per_domain=1, timeout=2)
        for r in runners:
            await r.cleanup()
        return dict(zip(hosts, recs, strict=True)), seen_ua

    recs, seen_ua = asyncio.run(run())
    out = {k: rules.robots_outcome(r["robots_status"], r["robots_error"]) for k, r in recs.items()}
    assert out == {
        "ok": "ok",
        "missing": "no_robots",
        "denied": "robots_error",
        "broken": "unreachable",
        "loop": "unreachable",
        "slow": "unreachable",
        "ua": "no_robots",
    }
    assert recs["ok"]["scheme"] == "http" and recs["ok"]["robots_body"].startswith(b"User-agent: GPTBot")
    assert recs["ok"]["tdmrep_status"] == 200 and recs["ok"]["aitxt_status"] == 404
    assert recs["loop"]["robots_error"] == "redirects"
    assert recs["slow"]["robots_error"] == "timeout"
    assert "tdmrep_status" not in recs["broken"]  # unreachable: nothing more asked
    assert seen_ua and all(u == rules.USER_AGENT for u in seen_ua)


def test_fetch_bad_host():
    recs = asyncio.run(fetch.fetch_all([("bücher.de", "bücher.de")], concurrency=1, per_domain=1))
    assert recs[0]["robots_error"] == "url"


def test_fetch_bug_is_not_an_outcome(monkeypatch):
    # only network and protocol failures become a host's outcome; a programming error stops the run
    async def boom(*a, **k):
        raise TypeError("a bug")

    monkeypatch.setattr(fetch, "get", boom)
    try:
        asyncio.run(fetch.fetch_all([("example.nl", "example.nl")], concurrency=1, per_domain=1))
    except TypeError:
        return
    raise AssertionError("TypeError was swallowed")


def test_get_classifies_network_errors():
    async def run():
        import aiohttp

        async with aiohttp.ClientSession() as s:
            return await fetch.get(s, "http://127.0.0.1:9/robots.txt", 100)  # port 9 (discard): refused

    r = asyncio.run(run())
    assert r["status"] is None and r["error"] == "connect"


def test_sparse_file_overlaps():
    data = bytes(range(256)) * 40
    segs = [(100, data[100:300]), (0, data[0:150]), (250, data[250:260]), (290, data[290:500]), (600, data[600:700])]
    f = hfread._SparseFile(len(data), segs)
    assert [(s, len(d)) for s, d in f.segments] == [(0, 500), (600, 100)]
    f.seek(120)
    assert f.read(300) == data[120:420]
    f.seek(650)
    assert f.read(20) == data[650:670]
    f.seek(550)
    try:
        f.read(10)
    except OSError:
        return
    raise AssertionError("read outside the segments did not fail")


def test_fetch_waits_out_429():
    async def run():
        calls = {"n": 0}

        async def busy_then_ok(req):
            if req.path == "/robots.txt":
                calls["n"] += 1
                if calls["n"] <= 2:
                    return web.Response(status=429, headers={"Retry-After": "1"})
                return web.Response(text="User-agent: *\nDisallow: /x\n")
            return web.Response(status=404)

        async def always_busy(req):
            return web.Response(status=429, headers={"Retry-After": "1"})

        r1, h1 = await _serve(busy_then_ok)
        r2, h2 = await _serve(always_busy)
        recs = await fetch.fetch_all([(h1, "a"), (h2, "b")], concurrency=4, per_domain=1, timeout=5)
        await r1.cleanup()
        await r2.cleanup()
        return recs, calls["n"]

    (ok, busy), n = asyncio.run(run())
    assert ok["robots_status"] == 200 and ok["r429"] == 2 and n == 3
    assert busy["robots_status"] == 429 and busy["r429"] == 2
    assert rules.robots_outcome(busy["robots_status"], busy["robots_error"]) == rules.UNREACHABLE


def test_retry_after():
    assert fetch.retry_after("5") == 5 and fetch.retry_after(None) == 10 and fetch.retry_after("3600") == 60
    assert fetch.retry_after("Wed, 21 Oct 2015 07:28:00 GMT") == 1  # in the past
    assert fetch.retry_after("soon") == 10


def test_fetch_robots_only_and_shared_domain_limit():
    # attempt 3: robots.txt only; two batches at the same time through one Fetcher keep its per-domain limit
    async def run():
        seen, live = [], {"now": 0, "max": 0}

        async def handler(req):
            seen.append(req.path)
            live["now"] += 1
            live["max"] = max(live["max"], live["now"])
            await asyncio.sleep(0.05)
            live["now"] -= 1
            return web.Response(text="User-agent: *\nDisallow: /x\n")

        r, host = await _serve(handler)
        async with fetch.Fetcher(concurrency=16, per_domain=2, timeout=5, robots_only=True) as f:
            a, b = await asyncio.gather(f.fetch([(host, "d")] * 6), f.fetch([(host, "d")] * 6))
        await r.cleanup()
        return a + b, seen, live["max"]

    recs, seen, most = asyncio.run(run())
    assert len(recs) == 12 and all(x["robots_status"] == 200 for x in recs)
    assert set(seen) == {"/robots.txt"} and not any("tdmrep_status" in x or "aitxt_status" in x for x in recs)
    assert most == 2
