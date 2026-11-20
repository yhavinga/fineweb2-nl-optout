"""fineweb2-nl-optout: FineWeb-2 nld_Latn without the documents whose site reserved text and data mining.

Phases (each unit of work is claimed in the bucket, see gcs.py, so any number of processes on any number of VMs can
run a phase together):

  plan     the files of FineWeb-2 nld_Latn at REVISION, their row groups, the units of the other phases
  hosts    per unit of row groups: the documents per host (columns url and dump only)
  shards   all hosts with a document up to the crawl limit, grouped by registered domain into fetch shards
  lists    a dated snapshot of the Apertus blocklist, the Common Crawl Opt-Out Ledger, the piracy and agent lists
  fetch    per shard: robots.txt, tdmrep.json, ai.txt of every host (--attempt 1), then again for the unreachable
           ones (--attempt 2, at least --min-age seconds after the first)
  hosts3   the hosts whose robots.txt gave robots_error on attempts 1 and 2, in units for attempt 3
  fetch    --attempt 3, per unit of hosts3, from a server outside Google Cloud: robots.txt only (rules.py)
  skip3    after attempt 3 was stopped: the hosts of its units without a result, which keep robots_error (rules.py)
  table    the host table: one decision row per host (decide.py), with the list flags
  filter   per unit of row groups: every document up to the crawl limit with its decision (rules.py), personal data
           replaced (pii.py), in kept/ (optout false) or excluded/ (optout true); the tokens; the decision of every
           document in decisions/
  report   the counts of every step, the manifest
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import random
import sys
import time
from collections import Counter, defaultdict
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

import gcs
import rules

REPO_ID = "HuggingFaceFW/fineweb-2"
REVISION = "af9c13333eb981300149d5ca60a8e9d659b276b9"
CONFIG = "nld_Latn"
TOKENIZER_ID = "yhavinga/dutch-llama-tokenizer"
TOKENIZER_REV = "d6a3a55a4ddf51a05d98d6a07f170f61a3c7c1ff"
APERTUS_ID = "swiss-ai/robots-txt-blocked-domains-multilingual"
APERTUS_REV = "cf9228759e60e463a2fee5faabeeaa099b11b7b4"
LEDGER_SHEET = "1uavIZ-Y2ew-Vj7d0_pY67SGD2jcUCsZZ5rPy1qEyPzg"
LEDGER_CSV = f"https://docs.google.com/spreadsheets/d/{LEDGER_SHEET}/export?format=csv&gid=0"
FW2_COLUMNS = [
    "text",
    "id",
    "dump",
    "url",
    "date",
    "file_path",
    "language",
    "language_score",
    "language_script",
    "minhash_cluster_size",
    "top_langs",
]
HOST_UNIT_RGS = 1000
FILTER_UNIT_RGS = 100
FILTER_BATCH_RGS = 10
SHARD_HOSTS = 3000
BIG_DOMAIN_CHUNK = 1000  # a registered domain with more hosts is split into chunks of this many hosts
FETCH3_UNIT_HOSTS = 2000  # attempt 3: consecutive fetch shards up to this many hosts make one unit
HERE = Path(__file__).resolve().parent


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def log(*a):
    print(time.strftime("%F %T"), *a, flush=True)


def jdump(obj) -> bytes:
    return json.dumps(obj, ensure_ascii=False, indent=1, sort_keys=False).encode()


def table_bytes(t: pa.Table, compression="zstd") -> bytes:
    buf = io.BytesIO()
    pq.write_table(t, buf, compression=compression)
    return buf.getvalue()


def read_table(uri: str, columns=None) -> pa.Table:
    return pq.read_table(io.BytesIO(gcs.read_bytes(uri)), columns=columns)


def work_loop(args, phase: str, units: list[str], do_unit, done_uri, done_prefix: str, ready=None, poll=120):
    """Claims and does units until every unit is done (by this process or another one).

    A unit another process holds is waited for; when its claim gets older than --stale without a result, this process
    takes it over (gcs.claim)."""
    order = units[:]
    random.Random(f"{gcs.owner()}-{time.time()}").shuffle(order)
    n = 0
    while True:
        done = {u.rsplit("/", 1)[-1].split(".", 1)[0] for u in gcs.listing(done_prefix)}
        left = [u for u in order if u not in done]
        if not left:
            break
        took = False
        for u in left:
            if ready is not None and not ready(u):
                continue
            if gcs.claim(f"{args.root}/work/claims/{phase}/{u}", done_uri(u), args.stale):
                t0 = time.time()
                do_unit(u)
                n += 1
                took = True
                log(f"UNIT {phase} {u} done in {time.time() - t0:.0f}s ({n} by this process)")
        if not took:
            time.sleep(poll)
    log(f"END {phase}: every unit done, {n} by this process")


async def work_loop_async(args, phase, units, do_unit, done_uri, done_prefix, parallel: int, poll=120):
    """work_loop with up to `parallel` units at a time in this process; do_unit is a coroutine function and the bucket
    calls run in threads. Attempt 3 uses it so that its units share one fetcher, whose limits (hosts at a time, hosts
    per registered domain, the pause after a 429) then hold over all of them."""
    import asyncio

    order = units[:]
    random.Random(f"{gcs.owner()}-{time.time()}").shuffle(order)
    n = 0
    slots = asyncio.Semaphore(parallel)
    while True:
        listed = await asyncio.to_thread(gcs.listing, done_prefix)
        done = {u.rsplit("/", 1)[-1].split(".", 1)[0] for u in listed}
        left = [u for u in order if u not in done]
        if not left:
            break
        took = 0

        async def one(u):
            nonlocal n, took
            async with slots:
                if await asyncio.to_thread(gcs.claim, f"{args.root}/work/claims/{phase}/{u}", done_uri(u), args.stale):
                    t0 = time.time()
                    await do_unit(u)
                    n += 1
                    took += 1
                    log(f"UNIT {phase} {u} done in {time.time() - t0:.0f}s ({n} by this process)")

        await asyncio.gather(*(one(u) for u in left))
        if not took:
            await asyncio.sleep(poll)
    log(f"END {phase}: every unit done, {n} by this process")


# ------------------------------------------------------------------------------------------------------------ plan
def cmd_plan(args):
    import requests

    from hfread import HFParquet

    plan_uri = f"{args.root}/work/plan.json"
    if gcs.exists(plan_uri):
        log("plan.json exists")
        return
    files = []
    for split in ("train", "test"):
        url = f"https://huggingface.co/api/datasets/{REPO_ID}/tree/{REVISION}/data/{CONFIG}/{split}"
        items = requests.get(url, timeout=60).json()
        for it in sorted(items, key=lambda x: x["path"]):
            if it["path"].endswith(".parquet"):
                m = HFParquet(REPO_ID, REVISION, it["path"]).metadata
                files.append(
                    {
                        "path": it["path"],
                        "split": split,
                        "size": it["size"],
                        "rows": m.num_rows,
                        "row_groups": m.num_row_groups,
                    }
                )
                log(f"{it['path']}: {m.num_rows} rows, {m.num_row_groups} row groups")

    def units(rgs_per_unit, prefix):
        out = []
        for f in files:
            stem = Path(f["path"]).stem
            for a in range(0, f["row_groups"], rgs_per_unit):
                b = min(a + rgs_per_unit, f["row_groups"])
                out.append(
                    {"name": f"{f['split']}-{stem}-r{a:05d}", "path": f["path"], "split": f["split"], "rg": [a, b]}
                )
        return out

    plan = {
        "repo": REPO_ID,
        "revision": REVISION,
        "config": CONFIG,
        "made": now_iso(),
        "files": files,
        "host_units": units(HOST_UNIT_RGS, "h"),
        "filter_units": units(FILTER_UNIT_RGS, "f"),
    }
    gcs.write_bytes(plan_uri, jdump(plan), "application/json", only_new=True)
    log(
        f"PLAN {len(files)} files, {sum(f['rows'] for f in files)} rows, {len(plan['host_units'])} host units, "
        f"{len(plan['filter_units'])} filter units"
    )


def load_plan(args) -> dict:
    return json.loads(gcs.read_bytes(f"{args.root}/work/plan.json"))


# ------------------------------------------------------------------------------------------------------------ hosts
def cmd_hosts(args):
    from urllib.parse import urlsplit

    from decide import split_url
    from hfread import HFParquet

    plan = load_plan(args)
    units = {u["name"]: u for u in plan["host_units"]}
    out = f"{args.root}/work/hosts"

    def do(name):
        u = units[name]
        f = HFParquet(REPO_ID, REVISION, u["path"], threads=args.threads)
        t = f.read(list(range(*u["rg"])), columns=["dump", "url"])
        dumps = Counter(t.column("dump").to_pylist())
        le: Counter = Counter()
        gt: Counter = Counter()
        no_host = ports = 0
        for d, url in zip(t.column("dump").to_pylist(), t.column("url").to_pylist(), strict=False):
            host, _ = split_url(url or "")
            if not host:
                no_host += 1
                continue
            try:
                ports += urlsplit(url).port not in (None, 80, 443)
            except ValueError:
                ports += 1
            (le if d <= rules.CRAWL_LAST else gt)[host] += 1
        hosts = sorted(set(le) | set(gt))
        tab = pa.table(
            {
                "host": hosts,
                "n_le": pa.array([le[h] for h in hosts], pa.int64()),
                "n_gt": pa.array([gt[h] for h in hosts], pa.int64()),
            }
        )
        gcs.write_bytes(f"{out}/{name}.parquet", table_bytes(tab))
        gcs.write_bytes(
            f"{out}/{name}.json",
            jdump(
                {
                    "unit": name,
                    "rows": t.num_rows,
                    "dumps": dict(sorted(dumps.items())),
                    "no_host": no_host,
                    "non_default_port": ports,
                    "hosts": len(hosts),
                    "etag": f.etag,
                }
            ),
            "application/json",
        )

    work_loop(args, "hosts", list(units), do, lambda n: f"{out}/{n}.json", f"{out}/")


# ------------------------------------------------------------------------------------------------------------ shards
def _regdoms(hosts: list[str]) -> list[str]:
    import tldextract

    ext = tldextract.TLDExtract(suffix_list_urls=(), cache_dir=None, include_psl_private_domains=False)
    return [ext(h).registered_domain or h for h in hosts]


def cmd_shards(args):
    plan = load_plan(args)
    out = f"{args.root}/work"
    if gcs.exists(f"{out}/fetch-shards/index.json"):
        log("fetch shards exist")
        return
    missing = [u["name"] for u in plan["host_units"] if not gcs.exists(f"{out}/hosts/{u['name']}.json")]
    if missing:
        sys.exit(f"hosts phase not complete: {len(missing)} units missing, e.g. {missing[:3]}")
    with ProcessPoolExecutor(args.processes) as ex:
        tabs = list(ex.map(read_table, [f"{out}/hosts/{u['name']}.parquet" for u in plan["host_units"]]))
    t = pa.concat_tables(tabs).group_by("host").aggregate([("n_le", "sum"), ("n_gt", "sum")])
    t = t.rename_columns(["host", "n_le", "n_gt"]).sort_by("host")
    hosts = t.column("host").to_pylist()
    chunks = [hosts[i : i + 200000] for i in range(0, len(hosts), 200000)]
    with ProcessPoolExecutor(args.processes) as ex:
        regdom = [r for part in ex.map(_regdoms, chunks) for r in part]
    t = t.append_column("regdom", pa.array(regdom, pa.string()))
    gcs.write_bytes(f"{out}/hosts-all.parquet", table_bytes(t))
    log(
        f"HOSTS {t.num_rows} hosts, {pc.sum(t['n_le']).as_py()} documents up to {rules.CRAWL_LAST}, "
        f"{pc.sum(t['n_gt']).as_py()} later"
    )
    f = t.filter(pc.greater(t["n_le"], 0))
    by_dom: dict[str, list[int]] = defaultdict(list)
    for i, d in enumerate(f.column("regdom").to_pylist()):
        by_dom[d].append(i)
    groups = []  # (rows, per-domain cap)
    small: list[int] = []
    for d in sorted(by_dom, key=lambda d: -len(by_dom[d])):
        rows = by_dom[d]
        if len(rows) > BIG_DOMAIN_CHUNK:
            groups += [
                (rows[i : i + BIG_DOMAIN_CHUNK], args.big_per_domain) for i in range(0, len(rows), BIG_DOMAIN_CHUNK)
            ]
        else:
            small += rows
            if len(small) >= SHARD_HOSTS:
                groups.append((small, args.per_domain))
                small = []
    if small:
        groups.append((small, args.per_domain))
    index = []
    for k, (rows, cap) in enumerate(groups):
        name = f"s{k:05d}"
        st = f.take(pa.array(rows)).select(["host", "regdom", "n_le"])
        gcs.write_bytes(f"{out}/fetch-shards/{name}.parquet", table_bytes(st))
        index.append({"name": name, "hosts": st.num_rows, "per_domain": cap})
    gcs.write_bytes(
        f"{out}/fetch-shards/index.json",
        jdump({"made": now_iso(), "hosts": f.num_rows, "shards": index}),
        "application/json",
    )
    log(f"SHARDS {len(index)} fetch shards, {f.num_rows} hosts")


# ------------------------------------------------------------------------------------------------------------ lists
def cmd_lists(args):
    import requests

    out = f"{args.root}/lists"
    if gcs.exists(f"{out}/manifest.json"):
        log("lists snapshot exists")
        return
    items = []

    def put(name, data: bytes, source: str, **extra):
        gcs.write_bytes(f"{out}/{name}", data)
        items.append(
            {
                "file": name,
                "source": source,
                "retrieved_at": now_iso(),
                "bytes": len(data),
                "sha256": hashlib.sha256(data).hexdigest(),
                **extra,
            }
        )

    r = requests.get(f"https://huggingface.co/datasets/{APERTUS_ID}/resolve/{APERTUS_REV}/fw2_domains.txt", timeout=120)
    r.raise_for_status()
    put("apertus-fw2_domains.txt", r.content, f"https://huggingface.co/datasets/{APERTUS_ID}", revision=APERTUS_REV)
    r = requests.get(LEDGER_CSV, timeout=120)
    r.raise_for_status()
    first = r.content.decode("utf-8", "replace").splitlines()[:2]
    put("cc-optout-ledger.csv", r.content, f"https://docs.google.com/spreadsheets/d/{LEDGER_SHEET}", sheet_header=first)
    for f, src in (
        ("piracy_domains.tsv", "lists/piracy_domains.tsv in this repository"),
        ("piracy_sources.tsv", "lists/piracy_sources.tsv in this repository"),
        (
            Path(rules.EXTENDED_SOURCE).name,
            f"github.com/ai-robots-txt/ai.robots.txt robots.json @ " f"{rules.EXTENDED_COMMIT}",
        ),
    ):
        put(f, (HERE / "lists" / f).read_bytes(), src)
    core, ext = rules.agent_tokens(HERE)
    gcs.write_bytes(
        f"{out}/manifest.json",
        jdump({"made": now_iso(), "files": items, "agents_core": sorted(core), "agents_extended": sorted(ext)}),
        "application/json",
        only_new=True,
    )
    log(f"LISTS {[(i['file'], i['bytes']) for i in items]}")


# ------------------------------------------------------------------------------------------------------------ fetch
RAW_SCHEMA = pa.schema(
    [
        ("host", pa.string()),
        ("regdom", pa.string()),
        ("n_le", pa.int64()),
        ("attempt", pa.int8()),
        ("fetched_at", pa.string()),
        ("scheme", pa.string()),
        ("robots_status", pa.int16()),
        ("robots_error", pa.string()),
        ("robots_detail", pa.string()),
        ("robots_url", pa.string()),
        ("robots_body", pa.binary()),
        ("robots_truncated", pa.bool_()),
        ("robots_ms", pa.int32()),
        ("tdmrep_status", pa.int16()),
        ("tdmrep_error", pa.string()),
        ("tdmrep_body", pa.binary()),
        ("aitxt_status", pa.int16()),
        ("aitxt_error", pa.string()),
        ("aitxt_body", pa.binary()),
        ("r429", pa.int8()),
    ]
)


def cmd_fetch(args):
    import asyncio

    import fetch

    if args.attempt == 3:
        return fetch_attempt3(args)
    index = json.loads(gcs.read_bytes(f"{args.root}/work/fetch-shards/index.json"))
    shards = {s["name"]: s for s in index["shards"]}
    w = f"{args.root}/work"
    out = f"{w}/fetch{args.attempt}"
    first_done: dict[str, float] = {}

    def ready(name):
        if args.attempt == 1:
            return True
        if name not in first_done:
            b = gcs.blob(f"{w}/fetch1/{name}.parquet")
            if not b.exists():
                return False
            b.reload()
            first_done[name] = b.time_created.timestamp()
        return time.time() - first_done[name] >= args.min_age

    def do(name):
        s = shards[name]
        if args.attempt == 1:
            src = read_table(f"{w}/fetch-shards/{name}.parquet")
        else:
            prev = read_table(
                f"{w}/fetch1/{name}.parquet", columns=["host", "regdom", "n_le", "robots_status", "robots_error"]
            )
            keep = [
                rules.robots_outcome(st, er) == rules.UNREACHABLE
                for st, er in zip(prev["robots_status"].to_pylist(), prev["robots_error"].to_pylist(), strict=False)
            ]
            src = prev.filter(pa.array(keep)).select(["host", "regdom", "n_le"])
        hosts = list(zip(src["host"].to_pylist(), src["regdom"].to_pylist(), strict=False))
        t0 = time.time()
        recs = (
            asyncio.run(
                fetch.fetch_all(
                    hosts,
                    args.concurrency,
                    s["per_domain"] if args.attempt == 1 else min(s["per_domain"], 2),
                    timeout=args.timeout,
                )
            )
            if hosts
            else []
        )
        n_le = src["n_le"].to_pylist()
        cols = {f.name: [] for f in RAW_SCHEMA}
        for (_h, d), n, r in zip(hosts, n_le, recs, strict=False):
            r = dict(r, regdom=d, n_le=n, attempt=args.attempt)
            for f in RAW_SCHEMA:
                cols[f.name].append(r.get(f.name))
        tab = pa.table(cols, schema=RAW_SCHEMA)
        outcomes = Counter(
            rules.robots_outcome(st, er) for st, er in zip(cols["robots_status"], cols["robots_error"], strict=False)
        )
        gcs.write_bytes(f"{out}/{name}.parquet", table_bytes(tab))
        log(
            f"FETCH {name} attempt {args.attempt}: {len(hosts)} hosts in {time.time() - t0:.0f}s "
            f"({len(hosts) / max(1e-9, time.time() - t0):.0f}/s) {dict(outcomes)}"
        )

    work_loop(args, f"fetch{args.attempt}", list(shards), do, lambda n: f"{out}/{n}.parquet", f"{out}/", ready)


# ------------------------------------------------------------------------------------------------------------ hosts3
FETCH3_INPUT = ["host", "regdom", "n_le", "attempt", "robots_status", "robots_error"]


def robots_error_hosts(job) -> pa.Table:
    """The hosts of one fetch shard whose robots.txt gave robots_error on the attempt that decided (the second for a
    host retried), with that attempt and its status."""
    root, name = job
    w = f"{root}/work"
    first = read_table(f"{w}/fetch1/{name}.parquet", columns=FETCH3_INPUT)
    retry = read_table(f"{w}/fetch2/{name}.parquet", columns=FETCH3_INPUT)
    t = pa.concat_tables([first.filter(pc.invert(pc.is_in(first["host"], value_set=retry["host"]))), retry])
    keep = [
        rules.robots_outcome(st, er) == rules.ERROR
        for st, er in zip(t["robots_status"].to_pylist(), t["robots_error"].to_pylist(), strict=True)
    ]
    t = t.filter(pa.array(keep, pa.bool_())).sort_by("host")
    t = t.select(["host", "regdom", "n_le", "attempt", "robots_status"])
    return t.add_column(0, "shard", pa.array([name] * t.num_rows, pa.string()))


def cmd_hosts3(args):
    w = f"{args.root}/work"
    uri = f"{w}/fetch3-hosts.parquet"
    if gcs.exists(uri):
        log("fetch3-hosts.parquet exists")
        return
    index = json.loads(gcs.read_bytes(f"{w}/fetch-shards/index.json"))
    names = [s["name"] for s in index["shards"]]
    for a in (1, 2):
        missing = [n for n in names if not gcs.exists(f"{w}/fetch{a}/{n}.parquet")]
        if missing:
            sys.exit(f"fetch attempt {a} not complete: {len(missing)} shards missing, e.g. {missing[:3]}")
    with ProcessPoolExecutor(args.processes) as ex:
        tabs = list(ex.map(robots_error_hosts, [(args.root, n) for n in names], chunksize=4))
    # a unit is consecutive shards, so that a registered domain is in one unit or in a few (a large one in chunks)
    out, k, size = [], 0, 0
    for t in tabs:
        if not t.num_rows:
            continue
        if size >= FETCH3_UNIT_HOSTS:
            k, size = k + 1, 0
        out.append(t.add_column(0, "unit", pa.array([f"u{k:04d}"] * t.num_rows, pa.string())))
        size += t.num_rows
    t = pa.concat_tables(out)
    gcs.write_bytes(uri, table_bytes(t), only_new=True)
    log(
        f"HOSTS3 {t.num_rows} hosts with robots_error, {pc.sum(t['n_le']).as_py()} documents, in "
        f"{len(set(t['shard'].to_pylist()))} shards and {k + 1} units; status "
        f"{dict(Counter(t['robots_status'].to_pylist()).most_common(8))}, attempt "
        f"{dict(Counter(t['attempt'].to_pylist()))}"
    )


RAW3_SCHEMA = pa.schema([pa.field("shard", pa.string()), *RAW_SCHEMA])


def fetch_attempt3(args):
    """Attempt 3, robots.txt only, of the hosts of hosts3; up to --parallel units at a time through one fetcher."""
    import asyncio

    import fetch

    w = f"{args.root}/work"
    out = f"{w}/fetch3"
    plan = read_table(f"{w}/fetch3-hosts.parquet", columns=["unit", "shard", "host", "regdom", "n_le"]).to_pylist()
    by_unit: dict[str, list[dict]] = defaultdict(list)
    for r in plan:
        by_unit[r["unit"]].append(r)
    log(f"attempt 3: {len(plan)} hosts in {len(by_unit)} units")

    async def run():
        async with fetch.Fetcher(args.concurrency, args.per_domain, args.timeout, robots_only=True) as f:

            async def do(name):
                rows = by_unit[name]
                t0 = time.time()
                recs = await f.fetch([(r["host"], r["regdom"]) for r in rows])
                cols = {x.name: [] for x in RAW3_SCHEMA}
                for r, rec in zip(rows, recs, strict=True):
                    rec = dict(rec, shard=r["shard"], regdom=r["regdom"], n_le=r["n_le"], attempt=3)
                    for x in RAW3_SCHEMA:
                        cols[x.name].append(rec.get(x.name))
                data = table_bytes(pa.table(cols, schema=RAW3_SCHEMA))
                await asyncio.to_thread(gcs.write_bytes, f"{out}/{name}.parquet", data)
                outcomes = Counter(
                    rules.robots_outcome(st, er)
                    for st, er in zip(cols["robots_status"], cols["robots_error"], strict=True)
                )
                answers = Counter(
                    str(st) if st is not None else er
                    for st, er in zip(cols["robots_status"], cols["robots_error"], strict=True)
                )
                dt = time.time() - t0
                log(
                    f"FETCH {name} attempt 3: {len(rows)} hosts in {dt:.0f}s ({len(rows) / max(1e-9, dt):.1f}/s), "
                    f"{len(data)} bytes, {dict(outcomes)}, {dict(answers.most_common(8))}, "
                    f"429 waited out {sum(cols['r429'])}"
                )

            await work_loop_async(
                args, "fetch3", sorted(by_unit), do, lambda n: f"{out}/{n}.parquet", f"{out}/", args.parallel
            )

    asyncio.run(run())


SKIP3_COLUMNS = ["unit", "host", "regdom", "n_le"]


def cmd_skip3(args):
    """work/fetch3-skipped.parquet: the hosts of the units of attempt 3 without a result (rules.py). Run once, after
    every attempt-3 process is stopped; a unit that has a result counts, whenever it came."""
    w = f"{args.root}/work"
    uri = f"{w}/fetch3-skipped.parquet"
    if gcs.exists(uri):
        sys.exit(f"{uri} exists")
    plan3 = read_table(f"{w}/fetch3-hosts.parquet", columns=SKIP3_COLUMNS)
    units3 = sorted(set(plan3["unit"].to_pylist()))
    missing = [u for u in units3 if not gcs.exists(f"{w}/fetch3/{u}.parquet")]
    if not missing:
        sys.exit("every unit of attempt 3 has a result: nothing to skip")
    t = plan3.filter(pc.is_in(plan3["unit"], value_set=pa.array(missing, pa.string()))).sort_by(
        [("unit", "ascending"), ("host", "ascending")]
    )
    gcs.write_bytes(uri, table_bytes(t), only_new=True)
    log(
        f"SKIP3 {len(missing)} of {len(units3)} units without a result ({' '.join(missing)}): {t.num_rows} hosts, "
        f"{pc.sum(t['n_le']).as_py()} documents, in {uri}"
    )


# ------------------------------------------------------------------------------------------------------------ table
TABLE_SCHEMA = pa.schema(
    [
        ("host", pa.string()),
        ("regdom", pa.string()),
        ("n_le", pa.int64()),
        ("attempts", pa.int8()),
        ("fetched_at", pa.string()),
        ("fetched_at_1", pa.string()),
        ("fetched_at_2", pa.string()),
        ("fetched_at_3", pa.string()),
        ("decided_by", pa.int8()),
        ("scheme", pa.string()),
        ("robots_status", pa.int16()),
        ("robots_error", pa.string()),
        ("robots_status_3", pa.int16()),
        ("robots_error_3", pa.string()),
        ("robots_url", pa.string()),
        ("robots_sha256", pa.string()),
        ("tdmrep_status", pa.int16()),
        ("aitxt_status", pa.int16()),
        ("host_status", pa.string()),
        ("robots_rules", pa.string()),
        ("signal", pa.bool_()),
        ("usage", pa.string()),
        ("named", pa.string()),
        ("root_blocked", pa.string()),
        ("tdm_valid", pa.bool_()),
        ("tdm_rules", pa.string()),
        ("ai_valid", pa.bool_()),
        ("ai_text", pa.bool_()),
        ("ai_rules", pa.string()),
        ("apertus", pa.bool_()),
        ("ledger", pa.bool_()),
        ("ledger_paths", pa.string()),
        ("piracy", pa.bool_()),
    ]
)

_LISTS = None


def _load_lists(root: str):
    global _LISTS
    if _LISTS is None:
        from lists import Apertus, Ledger, Piracy

        _LISTS = (
            Apertus(gcs.read_bytes(f"{root}/lists/apertus-fw2_domains.txt").decode("utf-8", "replace")),
            Ledger(gcs.read_bytes(f"{root}/lists/cc-optout-ledger.csv").decode("utf-8", "replace")),
            Piracy(gcs.read_bytes(f"{root}/lists/piracy_domains.tsv").decode("utf-8")),
            rules.agent_tokens(HERE),
        )
    return _LISTS


# what attempt 3 replaces when it decides (rules.attempt3_decides); tdmrep.json and ai.txt stay those of before
ROBOTS3_FIELDS = [
    "fetched_at",
    "scheme",
    "robots_status",
    "robots_error",
    "robots_detail",
    "robots_url",
    "robots_body",
    "robots_truncated",
    "robots_ms",
]


def table_shard(job) -> pa.Table:
    import decide

    # third: the attempt-3 records of this shard's hosts, by host; skipped: its hosts in fetch3-skipped.parquet
    root, name, third, skipped = job
    third, skipped = dict(third), set(skipped)
    apertus, ledger, piracy, (core, ext) = _load_lists(root)
    w = f"{root}/work"
    first = read_table(f"{w}/fetch1/{name}.parquet").to_pylist()
    retry_uri = f"{w}/fetch2/{name}.parquet"
    retry = {r["host"]: r for r in read_table(retry_uri).to_pylist()}
    cols = {f.name: [] for f in TABLE_SCHEMA}
    for r1 in first:
        host, regdom = r1["host"], r1["regdom"]
        r = retry.get(host, r1)
        decided_by = 2 if host in retry else 1
        r3 = third.pop(host, None)
        error = rules.robots_outcome(r.get("robots_status"), r.get("robots_error")) == rules.ERROR
        if host in skipped:
            skipped.discard(host)
            if not error or r3 is not None:
                raise RuntimeError(f"{name}: {host}: in fetch3-skipped.parquet, so robots_error without attempt 3")
        elif error != (r3 is not None):
            raise RuntimeError(f"{name}: {host}: attempt 3 must have exactly the hosts with robots_error before it")
        if r3 is not None and rules.attempt3_decides(r3["robots_status"], r3["robots_error"]):
            r = {**r, **{k: r3[k] for k in ROBOTS3_FIELDS}}
            decided_by = 3
        d = decide.decide_host(r, core, ext)
        body = r.get("robots_body")
        row = {
            "host": host,
            "regdom": regdom,
            "n_le": r1["n_le"],
            "attempts": 1 + (host in retry) + (r3 is not None),
            "fetched_at": r["fetched_at"],
            "fetched_at_1": r1["fetched_at"],
            "fetched_at_2": retry[host]["fetched_at"] if host in retry else None,
            "fetched_at_3": r3["fetched_at"] if r3 is not None else None,
            "decided_by": decided_by,
            "scheme": r.get("scheme"),
            "robots_status": r.get("robots_status"),
            "robots_error": r.get("robots_error"),
            "robots_status_3": r3["robots_status"] if r3 is not None else None,
            "robots_error_3": r3["robots_error"] if r3 is not None else None,
            "robots_url": r.get("robots_url"),
            "robots_sha256": hashlib.sha256(body).hexdigest() if body is not None else None,
            "tdmrep_status": r.get("tdmrep_status"),
            "aitxt_status": r.get("aitxt_status"),
            **d,
            "apertus": apertus.match(host),
            "ledger": ledger.match_host(host),
            "ledger_paths": json.dumps(ledger.prefixes_for(host)) if ledger.prefixes_for(host) else None,
            "piracy": piracy.match(host, regdom),
        }
        for f in TABLE_SCHEMA:
            cols[f.name].append(row[f.name])
    if third:
        raise RuntimeError(f"{name}: attempt 3 has {len(third)} hosts that attempt 1 did not, e.g. {list(third)[:3]}")
    if skipped:
        raise RuntimeError(
            f"{name}: fetch3-skipped.parquet has {len(skipped)} hosts not in it, e.g. {sorted(skipped)[:3]}"
        )
    return pa.table(cols, schema=TABLE_SCHEMA)


def cmd_table(args):
    w = f"{args.root}/work"
    index = json.loads(gcs.read_bytes(f"{w}/fetch-shards/index.json"))
    names = [s["name"] for s in index["shards"]]
    for a in (1, 2):
        missing = [n for n in names if not gcs.exists(f"{w}/fetch{a}/{n}.parquet")]
        if missing:
            sys.exit(f"fetch attempt {a} not complete: {len(missing)} shards missing, e.g. {missing[:3]}")
    if not gcs.exists(f"{w}/fetch3-hosts.parquet"):
        sys.exit("attempt 3 is not planned: run hosts3 and fetch --attempt 3 first")
    plan3 = read_table(f"{w}/fetch3-hosts.parquet", columns=["unit", "shard", "host"])
    units3 = sorted(set(plan3["unit"].to_pylist()))
    # the units of attempt 3 that were stopped without a result (rules.py): their hosts keep robots_error
    skipped_uri = f"{w}/fetch3-skipped.parquet"
    sk = read_table(skipped_uri, columns=SKIP3_COLUMNS) if gcs.exists(skipped_uri) else None
    sk_units = sorted(set(sk["unit"].to_pylist())) if sk is not None else []
    done3 = [u for u in units3 if gcs.exists(f"{w}/fetch3/{u}.parquet")]
    missing = [u for u in units3 if u not in done3 and u not in sk_units]
    if missing:
        sys.exit(f"fetch attempt 3 not complete: {len(missing)} of {len(units3)} units missing, e.g. {missing[:3]}")
    bad = sorted(set(sk_units) & set(done3) | set(sk_units) - set(units3))
    if bad:
        sys.exit(f"{skipped_uri}: units that have a result or are not in fetch3-hosts.parquet: {bad[:5]}")
    skipped: dict[str, set] = defaultdict(set)
    if sk is not None:
        planned = plan3.filter(pc.is_in(plan3["unit"], value_set=pa.array(sk_units, pa.string())))
        if sorted(planned["host"].to_pylist()) != sorted(sk["host"].to_pylist()):
            sys.exit(f"{skipped_uri}: the hosts differ from those of its units in fetch3-hosts.parquet")
        for shard, host in zip(planned["shard"].to_pylist(), planned["host"].to_pylist(), strict=True):
            skipped[shard].add(host)
        log(f"attempt 3: {len(sk_units)} units skipped, {sk.num_rows} hosts keep robots_error ({skipped_uri})")
    third: dict[str, dict] = defaultdict(dict)
    with ThreadPoolExecutor(8) as ex:
        for t3 in ex.map(lambda u: read_table(f"{w}/fetch3/{u}.parquet"), done3):
            for r in t3.select(["shard", "host", *ROBOTS3_FIELDS]).to_pylist():
                third[r["shard"]][r["host"]] = r
    n3 = sum(len(v) for v in third.values())
    n_sk = sk.num_rows if sk is not None else 0
    if n3 + n_sk != plan3.num_rows:
        sys.exit(f"attempt 3: {n3} hosts in the results and {n_sk} skipped, {plan3.num_rows} in fetch3-hosts.parquet")
    jobs = [(args.root, n, third.pop(n, {}), skipped.pop(n, set())) for n in names]
    if third or skipped:
        sys.exit(f"attempt 3 has shards that are not in the index: {sorted(set(third) | set(skipped))[:3]}")
    with ProcessPoolExecutor(args.processes) as ex:
        tabs = list(ex.map(table_shard, jobs, chunksize=4))
    t = pa.concat_tables(tabs).sort_by("host")
    if len(set(t["host"].to_pylist())) != t.num_rows:
        sys.exit("host table: a host occurs twice")
    sink = io.BytesIO()
    with pa.ipc.new_file(sink, t.schema) as wr:
        wr.write_table(t, max_chunksize=1 << 20)
    gcs.write_bytes(f"{w}/hosts-table.arrow", sink.getvalue())
    gcs.write_bytes(f"{args.root}/optout/hosts.parquet", table_bytes(t))
    log(
        f"TABLE {t.num_rows} hosts, host_status {dict(Counter(t['host_status'].to_pylist()))}, decided by attempt "
        f"{dict(Counter(t['decided_by'].to_pylist()))}"
    )


# ------------------------------------------------------------------------------------------------------------ filter
OUT_EXTRA = [
    ("source_file", pa.string()),
    ("source_row", pa.int64()),
    ("optout", pa.bool_()),
    ("optout_reasons", pa.list_(pa.string())),
    ("host_status", pa.string()),
    ("optout_checked_at", pa.timestamp("s", tz="UTC")),
    ("pii_replaced", pa.int32()),
]
# optout false and true apart, so that kept/ can be used or shared without the excluded documents
OUT_SETS = ["kept", "excluded"]


def cmd_filter(args):
    from tokenizers import Tokenizer

    import decide
    import pii
    from hfread import HFParquet

    plan = load_plan(args)
    units = {u["name"]: u for u in plan["filter_units"]}
    table = pa.ipc.open_file(pa.memory_map(args.table)).read_all()
    tok = Tokenizer.from_file(args.tokenizer)
    stats_dir = stats_uri(args.root, args.run)
    robots_only = set(rules.ROBOTS_REASONS)

    def ntokens(texts: list[str]) -> list[int]:
        return [len(e.ids) for e in tok.encode_batch(texts, add_special_tokens=False)]

    def do(name):
        u = units[name]
        f = HFParquet(REPO_ID, REVISION, u["path"], threads=args.threads)
        base = sum(f.metadata.row_group(i).num_rows for i in range(u["rg"][0]))
        out_uri = {k: f"{args.root}/{k}/{u['split']}/{name}.parquet" for k in OUT_SETS}
        fw2_schema = f.metadata.schema.to_arrow_schema()
        out_schema = pa.schema(list(fw2_schema) + [pa.field(n, t) for n, t in OUT_EXTRA])
        st = {
            "unit": name,
            "path": u["path"],
            "rg": u["rg"],
            "etag": f.etag,
            "code": HERE.name,
            "docs": 0,
            "crawl": 0,
            "in_limit": 0,
            "kept": 0,
            "excluded": 0,
            "first": Counter(),
            "any": Counter(),
            "only": Counter(),
            "tokens_first": Counter(),
            "chars_first": Counter(),
            "host_status": Counter(),
            "host_status_kept": Counter(),
            "tokens_in": 0,
            "tokens_crawl": 0,
            "tokens_kept": 0,
            "chars_in": 0,
            "chars_crawl": 0,
            "chars_kept": 0,
            "pii": Counter(),
            "pii_docs": 0,
            "pii_kept": Counter(),
            "pii_docs_kept": 0,
            # documents of hosts that attempt 3 made ok whose only reasons are robots reasons: kept on attempts 1-2
            "a3_excluded": Counter(),
            "a3_excluded_tokens": Counter(),
            "a3_excluded_hosts": defaultdict(lambda: [0, 0]),
        }
        dec_cols = {"id": [], "source_row": [], "reason": [], "reasons": []}
        evals: dict[str, decide.HostEval] = {}
        with (
            gcs.blob(out_uri["kept"]).open("wb", chunk_size=32 * 1024 * 1024, ignore_flush=True) as fk,
            gcs.blob(out_uri["excluded"]).open("wb", chunk_size=32 * 1024 * 1024, ignore_flush=True) as fx,
            pq.ParquetWriter(fk, out_schema, compression="zstd") as wk,
            pq.ParquetWriter(fx, out_schema, compression="zstd") as wx,
        ):
            writers = {"kept": wk, "excluded": wx}
            rgs = list(range(*u["rg"]))
            row0 = base
            for b in range(0, len(rgs), FILTER_BATCH_RGS):
                t = f.read(rgs[b : b + FILTER_BATCH_RGS])
                texts, urls, dumps, ids = (t.column(c).to_pylist() for c in ("text", "url", "dump", "id"))
                inl = [d <= rules.CRAWL_LAST for d in dumps]
                hp = [decide.split_url(x or "") if ok else ("", "") for x, ok in zip(urls, inl, strict=True)]
                new = sorted({h for (h, _), ok in zip(hp, inl, strict=True) if ok and h and h not in evals})
                if new:
                    for r in table.filter(pc.is_in(table["host"], value_set=pa.array(new))).to_pylist():
                        evals[r["host"]] = decide.HostEval(r)
                    missing = [h for h in new if h not in evals]
                    if missing:  # every host with a document up to the crawl limit was fetched
                        raise RuntimeError(
                            f"{name}: {len(missing)} hosts of documents up to the crawl limit are not in the host "
                            f"table, e.g. {missing[:3]} (rows {row0}-{row0 + t.num_rows - 1})"
                        )
                sets = {
                    k: {"idx": [], "text": [], "reasons": [], "status": [], "at": [], "pii": [], "a3": []}
                    for k in OUT_SETS
                }
                crawl_texts = []
                for i, (text, ok, (h, path)) in enumerate(zip(texts, inl, hp, strict=True)):
                    dec_cols["id"].append(ids[i])
                    dec_cols["source_row"].append(row0 + i)
                    if not ok:
                        dec_cols["reason"].append(rules.CRAWL)
                        dec_cols["reasons"].append(rules.CRAWL)
                        crawl_texts.append(text)
                        st["chars_crawl"] += len(text)
                        continue
                    if h:
                        ev = evals[h]
                        reasons, status, at = ev.reasons(path), ev.host_status, ev.checked_at
                        a3 = ev.decided_by == 3 and bool(reasons) and set(reasons) <= robots_only
                    else:
                        reasons, status, at, a3 = [], rules.NO_HOST, None, False
                    clean, counts = pii.scrub(text)
                    n = sum(counts.values())
                    st["pii"].update(counts)
                    st["pii_docs"] += n > 0
                    st["host_status"][status] += 1
                    for r in reasons:
                        st["any"][r] += 1
                    if len(reasons) == 1:
                        st["only"][reasons[0]] += 1
                    if reasons:
                        st["first"][reasons[0]] += 1
                        st["chars_first"][reasons[0]] += len(clean)
                    else:
                        st["host_status_kept"][status] += 1
                        st["pii_kept"].update(counts)
                        st["pii_docs_kept"] += n > 0
                        st["chars_kept"] += len(clean)
                    if a3:
                        st["a3_excluded"][reasons[0]] += 1
                        st["a3_excluded_hosts"][h][0] += 1
                    st["chars_in"] += len(clean)
                    dec_cols["reason"].append(reasons[0] if reasons else "")
                    dec_cols["reasons"].append(",".join(reasons))
                    c = sets["excluded" if reasons else "kept"]
                    c["idx"].append(i)
                    c["text"].append(clean)
                    c["reasons"].append(reasons)
                    c["status"].append(status)
                    c["at"].append(at)
                    c["pii"].append(n)
                    c["a3"].append(h if a3 else None)
                if crawl_texts:
                    st["tokens_crawl"] += sum(ntokens(crawl_texts))
                st["chars_in"] += sum(len(x) for x in crawl_texts)
                for k in OUT_SETS:
                    c = sets[k]
                    if not c["idx"]:
                        continue
                    ntok = ntokens(c["text"])
                    st["tokens_in"] += sum(ntok)
                    if k == "kept":
                        st["tokens_kept"] += sum(ntok)
                    else:
                        for rs, n, a3h in zip(c["reasons"], ntok, c["a3"], strict=True):
                            st["tokens_first"][rs[0]] += n
                            if a3h:
                                st["a3_excluded_tokens"][rs[0]] += n
                                st["a3_excluded_hosts"][a3h][1] += n
                    st[k] += len(c["idx"])
                    kt = t.take(pa.array(c["idx"], pa.int64()))
                    kt = kt.set_column(kt.schema.get_field_index("text"), "text", pa.array(c["text"], pa.string()))
                    extra = {
                        "source_file": pa.array([u["path"]] * len(c["idx"]), pa.string()),
                        "source_row": pa.array([j + row0 for j in c["idx"]], pa.int64()),
                        "optout": pa.array([k == "excluded"] * len(c["idx"]), pa.bool_()),
                        "optout_reasons": pa.array(c["reasons"], pa.list_(pa.string())),
                        "host_status": pa.array(c["status"], pa.string()),
                        "optout_checked_at": pa.array(c["at"], pa.timestamp("s", tz="UTC")),
                        "pii_replaced": pa.array(c["pii"], pa.int32()),
                    }
                    for col, _ in OUT_EXTRA:
                        kt = kt.append_column(col, extra[col])
                    writers[k].write_table(kt.cast(out_schema), row_group_size=FILTER_BATCH_RGS * 1000)
                st["docs"] += t.num_rows
                st["crawl"] += len(crawl_texts)
                row0 += t.num_rows
        st["in_limit"] = st["kept"] + st["excluded"]
        st["tokens_in"] += st["tokens_crawl"]
        st["hosts"] = len(evals)
        dec = pa.table(
            {
                "id": dec_cols["id"],
                "source_file": [u["path"]] * len(dec_cols["id"]),
                "source_row": pa.array(dec_cols["source_row"], pa.int64()),
                "reason": pa.array(dec_cols["reason"]).dictionary_encode(),
                "reasons": pa.array(dec_cols["reasons"]).dictionary_encode(),
            }
        )
        gcs.write_bytes(f"{args.root}/decisions/{u['split']}/{name}.parquet", table_bytes(dec))
        st["out"] = {}
        for k in OUT_SETS:
            ob = gcs.blob(out_uri[k])
            ob.reload()
            st["out"][k] = {"uri": out_uri[k], "bytes": ob.size, "crc32c": ob.crc32c, "md5": ob.md5_hash}
        gcs.write_bytes(
            f"{stats_dir}/{name}.json",
            jdump({k: (dict(v) if isinstance(v, Counter) else v) for k, v in st.items()}),
            "application/json",
        )
        log(
            f"FILTER {name}: {st['docs']} docs, {st['crawl']} after the crawl limit, {st['kept']} kept, "
            f"{st['excluded']} excluded, first reasons {dict(st['first'])}, host_status {dict(st['host_status'])}, "
            f"pii {dict(st['pii'])}"
        )

    os.environ.setdefault("TOKENIZERS_PARALLELISM", "false")
    work_loop(args, f"filter-{args.run}", list(units), do, lambda n: f"{stats_dir}/{n}.json", f"{stats_dir}/")


def stats_uri(root: str, run: str) -> str:
    """The stats of the filter run `run`: a unit is done in that run when its stats exist there, so that a new run
    (after a new host table) does every unit again, and the stats of an earlier run stay."""
    if not run or "/" in run:
        raise ValueError(f"bad run name {run!r}")
    return f"{root}/work/stats-{run}"


# ------------------------------------------------------------------------------------------------------------ main
def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--root", default=os.environ.get("FW2OPTOUT_ROOT"),
                   help="gs://<bucket>/<prefix> of the run (default: $FW2OPTOUT_ROOT)")
    p.add_argument("--stale", type=float, default=7200, help="seconds after which a claim without result is taken")
    p.add_argument("--gsutil", action="store_true", help="bucket calls through gsutil (no application credentials)")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("plan")
    s = sub.add_parser("hosts")
    s.add_argument("--threads", type=int, default=16)
    s = sub.add_parser("shards")
    s.add_argument("--processes", type=int, default=16)
    s.add_argument("--per-domain", type=int, default=4)
    s.add_argument("--big-per-domain", type=int, default=8)
    sub.add_parser("lists")
    s = sub.add_parser("fetch")
    s.add_argument("--attempt", type=int, choices=[1, 2, 3], required=True)
    s.add_argument("--concurrency", type=int, default=256)
    s.add_argument("--timeout", type=float, default=30.0)
    s.add_argument("--min-age", type=float, default=1800, help="attempt 2: seconds after attempt 1 of the shard")
    s.add_argument("--per-domain", type=int, default=2, help="attempt 3: hosts of one registered domain at a time")
    s.add_argument("--parallel", type=int, default=6, help="attempt 3: units at a time, through one fetcher")
    s = sub.add_parser("hosts3")
    s.add_argument("--processes", type=int, default=16)
    sub.add_parser("skip3")
    s = sub.add_parser("table")
    s.add_argument("--processes", type=int, default=32)
    s = sub.add_parser("filter")
    s.add_argument("--table", required=True, help="local copy of work/hosts-table.arrow")
    s.add_argument("--tokenizer", required=True, help="local tokenizer.json of " + TOKENIZER_ID)
    s.add_argument("--threads", type=int, default=8)
    s.add_argument("--run", required=True, help="name of this filter run: its stats are in work/stats-<run>/")
    s = sub.add_parser("report")
    s.add_argument("--out", required=True, help="local directory for the report")
    s.add_argument("--processes", type=int, default=16)
    s.add_argument("--commit", required=True)
    s.add_argument("--run", required=True, help="the filter run to report (work/stats-<run>/)")
    args = p.parse_args(argv)
    if not args.root:
        p.error("set --root or FW2OPTOUT_ROOT to gs://<bucket>/<prefix> of the run")
    if args.gsutil:
        gcs.use_gsutil()
    if args.cmd == "report":
        import report

        report.run(args)
    else:
        globals()[f"cmd_{args.cmd}"](args)


if __name__ == "__main__":
    main()
