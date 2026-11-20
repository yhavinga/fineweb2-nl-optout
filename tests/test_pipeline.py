"""The phases end to end on a small fake FineWeb-2 file, with the bucket and the Hub replaced by memory."""

import io
import json
import types

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import fw2optout as fo
import gcs
import hfread
import rules


class FakeBlob:
    def __init__(self, store, uri):
        self.store, self.uri = store, uri

    def open(self, mode, **kw):
        store, uri = self.store, self.uri
        if mode == "rb":
            return io.BytesIO(store[uri])

        class W(io.BytesIO):
            def close(self):
                store[uri] = self.getvalue()
                super().close()

        return W()

    def reload(self):
        self.size = len(self.store[self.uri])
        self.crc32c = self.md5_hash = "x"

    def exists(self):
        return self.uri in self.store


@pytest.fixture
def bucket(monkeypatch):
    store = {}
    monkeypatch.setattr(gcs, "read_bytes", lambda uri: store[uri])
    monkeypatch.setattr(gcs, "write_bytes", lambda uri, data, *a, **k: store.__setitem__(uri, data))
    monkeypatch.setattr(gcs, "exists", lambda uri: uri in store)
    monkeypatch.setattr(gcs, "listing", lambda prefix: [u for u in store if u.startswith(prefix)])
    monkeypatch.setattr(gcs, "claim", lambda claim_uri, done_uri, stale: done_uri not in store)
    monkeypatch.setattr(gcs, "blob", lambda uri: FakeBlob(store, uri))
    return store


DOCS = [
    # (url, dump, text)
    ("https://www.nrc.nl/nieuws/a", "CC-MAIN-2020-05", "Nieuws. Bel 06-12345678."),
    ("https://blog.example.nl/post", "CC-MAIN-2020-05", "Een blog. Rekening NL91 ABNA 0417 1643 00 graag."),
    ("https://blog.example.nl/admin/x", "CC-MAIN-2020-05", "Beheer."),
    ("https://blog.example.nl/post2", "CC-MAIN-2024-10", "Later gecrawld."),
    ("https://only-late.nl/x", "CC-MAIN-2024-10", "Alleen laat."),
    ("https://dead.nl/x", "CC-MAIN-2019-04", "Dode site."),
    ("https://forbidden.nl/x", "CC-MAIN-2019-04", "403 op robots."),
    ("https://thepiratebay.org/x", "CC-MAIN-2019-04", "Torrents."),
    ("https://tdm.nl/news/x", "CC-MAIN-2019-04", "Voorbehouden."),
    ("https://tdm.nl/free/x", "CC-MAIN-2019-04", "Vrij. Kerkstraat 12, 1234 AB Utrecht."),
    ("https://signal.nl/x", "CC-MAIN-2019-04", "Content signal."),
    ("https://ad.nl/x", "CC-MAIN-2019-04", "Ledger."),
    ("https://petal.nl/x", "CC-MAIN-2019-04", "Alleen PetalBot geblokkeerd."),
    ("not a url", "CC-MAIN-2019-04", "Geen host."),
]
ROBOTS = {
    "www.nrc.nl": (200, b"User-agent: GPTBot\nDisallow: /\n"),
    "blog.example.nl": (200, b"User-agent: *\nDisallow: /admin/\n"),
    "dead.nl": (None, None),
    "forbidden.nl": (403, b""),
    "thepiratebay.org": (404, b""),
    "tdm.nl": (404, b""),
    "signal.nl": (200, b"User-agent: *\nContent-Signal: ai-train=no\nAllow: /\n"),
    "ad.nl": (200, b"User-agent: *\nAllow: /\n"),
    "petal.nl": (200, b"User-agent: PetalBot\nDisallow: /\n"),
}
TDM = {"tdm.nl": b'[{"location": "/free/", "tdm-reservation": 0}, {"location": "/", "tdm-reservation": 1}]'}


def fake_file():
    n = len(DOCS)
    rows = DOCS * 150  # 2100 rows, 3 row groups of 1000 / 1000 / 100
    t = pa.table(
        {
            "text": [d[2] for d in rows],
            "id": [f"<urn:uuid:{i}>" for i in range(len(rows))],
            "dump": [d[1] for d in rows],
            "url": [d[0] for d in rows],
            "date": ["2020-01-01T00:00:00Z"] * len(rows),
            "file_path": ["s3://x"] * len(rows),
            "language": ["nld"] * len(rows),
            "language_score": [0.99] * len(rows),
            "language_script": ["Latn"] * len(rows),
            "minhash_cluster_size": pa.array([1] * len(rows), pa.int64()),
            "top_langs": ["{}"] * len(rows),
        }
    )
    buf = io.BytesIO()
    pq.write_table(t, buf, row_group_size=1000, compression="zstd")
    assert n
    return t, buf.getvalue()


THIRD_AT = "2026-11-15T15:00:00Z"


class FakeFetcher:
    """fetch.Fetcher of attempt 3 with the answers of `answers` (host -> (status, body)) instead of the network."""

    answers: dict = {}

    def __init__(self, concurrency, per_domain, timeout=30.0, robots_only=False):
        assert robots_only and per_domain == 2 and concurrency == 24

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        pass

    async def fetch(self, hosts, progress=None):
        out = []
        for h, _ in hosts:
            st, body = self.answers[h]
            out.append(
                {
                    "host": h,
                    "fetched_at": THIRD_AT,
                    "scheme": "https",
                    "robots_status": st,
                    "robots_error": None if st else "dns",
                    "robots_url": f"https://{h}/robots.txt",
                    "robots_body": body,
                    "robots_truncated": False,
                    "robots_ms": 1,
                    "r429": 0,
                }
            )
        return out


def run_pipeline(bucket, monkeypatch, tmp_path, third):
    """Every phase on the fake file; `third` is the answer (status, body) of attempt 3 per robots_error host, or None
    when attempt 3 is stopped before its unit is done (skip3)."""
    t, data = fake_file()

    class Local(hfread.HFParquet):
        def _resolve(self):
            self.url, self.size, self.etag, self.commit = "mem", len(data), "etag", "c"

        def _get(self, start, end):
            return data[start:end]

    monkeypatch.setattr(hfread, "HFParquet", Local)
    root = "gs://b/r"
    path = "data/nld_Latn/train/000_00000.parquet"
    plan = {
        "repo": "HuggingFaceFW/fineweb-2",
        "revision": "rev",
        "config": "nld_Latn",
        "files": [{"path": path, "split": "train", "rows": t.num_rows, "row_groups": 3}],
        "host_units": [
            {"name": "train-000_00000-r00000", "path": path, "split": "train", "rg": [0, 2]},
            {"name": "train-000_00000-r00002", "path": path, "split": "train", "rg": [2, 3]},
        ],
        "filter_units": [{"name": "train-000_00000-r00000", "path": path, "split": "train", "rg": [0, 3]}],
    }
    bucket[f"{root}/work/plan.json"] = json.dumps(plan).encode()
    args = types.SimpleNamespace(root=root, stale=7200, threads=2, processes=2, per_domain=4, big_per_domain=8)

    fo.cmd_hosts(args)
    fo.cmd_shards(args)
    allhosts = pq.read_table(io.BytesIO(bucket[f"{root}/work/hosts-all.parquet"])).to_pylist()
    by = {r["host"]: r for r in allhosts}
    assert by["blog.example.nl"]["n_le"] == 300 and by["blog.example.nl"]["n_gt"] == 150
    assert by["only-late.nl"]["n_le"] == 0
    index = json.loads(bucket[f"{root}/work/fetch-shards/index.json"])
    shard_hosts = {
        h
        for s in index["shards"]
        for h in pq.read_table(io.BytesIO(bucket[f"{root}/work/fetch-shards/{s['name']}.parquet"]))["host"].to_pylist()
    }
    assert shard_hosts == set(ROBOTS)  # only hosts with a document up to the crawl limit

    # the fetch, from the tables above instead of the network
    for s in index["shards"]:
        src = pq.read_table(io.BytesIO(bucket[f"{root}/work/fetch-shards/{s['name']}.parquet"])).to_pylist()
        for attempt in (1, 2):
            cols = {f.name: [] for f in fo.RAW_SCHEMA}
            for r in src:
                st, body = ROBOTS[r["host"]]
                if attempt == 2 and st is not None:
                    continue
                rec = {
                    **r,
                    "attempt": attempt,
                    "fetched_at": "2026-11-14T16:00:00Z" if attempt == 1 else "2026-11-14T16:45:00Z",
                    "scheme": "https",
                    "robots_status": st,
                    "robots_error": None if st else "dns",
                    "robots_body": body,
                    "robots_truncated": False,
                    "robots_ms": 1,
                }
                if r["host"] in TDM:
                    rec.update(tdmrep_status=200, tdmrep_body=TDM[r["host"]])
                for f in fo.RAW_SCHEMA:
                    cols[f.name].append(rec.get(f.name))
            bucket[f"{root}/work/fetch{attempt}/{s['name']}.parquet"] = fo.table_bytes(
                pa.table(cols, schema=fo.RAW_SCHEMA)
            )
    bucket[f"{root}/lists/apertus-fw2_domains.txt"] = b"nothing.example\n"
    bucket[f"{root}/lists/cc-optout-ledger.csv"] = b"a,b,c\nLAST UPDATED,,\n,,\nDPG,2026-09-23,ad.nl\n"
    bucket[f"{root}/lists/piracy_domains.tsv"] = (fo.HERE / "lists" / "piracy_domains.tsv").read_bytes()
    fo._LISTS = None

    # attempt 3: the hosts with robots_error, through fetch_attempt3 with the network replaced
    fo.cmd_hosts3(args)
    plan3 = pq.read_table(io.BytesIO(bucket[f"{root}/work/fetch3-hosts.parquet"])).to_pylist()
    assert [(r["host"], r["robots_status"], r["attempt"], r["unit"]) for r in plan3] == [
        ("forbidden.nl", 403, 1, "u0000")
    ]
    if third is None:
        fo.cmd_skip3(args)
        sk = pq.read_table(io.BytesIO(bucket[f"{root}/work/fetch3-skipped.parquet"]))
        assert sk.column_names == ["unit", "host", "regdom", "n_le"]
        assert sk.to_pylist() == [{"unit": "u0000", "host": "forbidden.nl", "regdom": "forbidden.nl", "n_le": 150}]
    else:
        import fetch

        FakeFetcher.answers = third
        monkeypatch.setattr(fetch, "Fetcher", FakeFetcher)
        fo.fetch_attempt3(
            types.SimpleNamespace(
                root=root, stale=7200, attempt=3, concurrency=24, per_domain=2, timeout=30, parallel=2
            )
        )
        shards3 = pq.read_table(io.BytesIO(bucket[f"{root}/work/fetch3/u0000.parquet"]))["shard"].to_pylist()
        assert shards3 == ["s00000"]
        with pytest.raises(SystemExit, match="nothing to skip"):
            fo.cmd_skip3(args)
    fo.cmd_table(args)
    ht = {r["host"]: r for r in pq.read_table(io.BytesIO(bucket[f"{root}/optout/hosts.parquet"])).to_pylist()}
    ipc = tmp_path / "hosts-table.arrow"
    ipc.write_bytes(bucket[f"{root}/work/hosts-table.arrow"])

    tok = tmp_path / "tokenizer.json"
    from tokenizers import Tokenizer, models, pre_tokenizers

    tk = Tokenizer(models.WordLevel({"[UNK]": 0}, unk_token="[UNK]"))
    tk.pre_tokenizer = pre_tokenizers.Whitespace()
    tk.save(str(tok))
    fargs = types.SimpleNamespace(root=root, stale=7200, table=str(ipc), tokenizer=str(tok), threads=2, run="t1")
    fo.cmd_filter(fargs)

    st = json.loads(bucket[f"{root}/work/stats-t1/train-000_00000-r00000.json"])
    unit = "train/train-000_00000-r00000.parquet"
    kept = pq.read_table(io.BytesIO(bucket[f"{root}/kept/{unit}"]))
    excl = pq.read_table(io.BytesIO(bucket[f"{root}/excluded/{unit}"]))
    dec = pq.read_table(io.BytesIO(bucket[f"{root}/decisions/{unit}"])).to_pylist()

    import report

    bucket[f"{root}/lists/manifest.json"] = json.dumps(
        {
            "files": [
                {"file": "x.txt", "source": "s", "retrieved_at": "2026-11-14T16:00:00Z", "bytes": 1, "sha256": "0" * 64}
            ]
        }
    ).encode()
    rargs = types.SimpleNamespace(root=root, out=str(tmp_path / "rep"), commit="abc", processes=2, run="t1")
    report.run(rargs)
    md = (tmp_path / "rep" / "fineweb2-nl-optout.md").read_text()
    js = json.loads((tmp_path / "rep" / "fineweb2-nl-optout.json").read_text())
    man = json.loads(bucket[f"{root}/manifest.json"])
    return types.SimpleNamespace(t=t, st=st, kept=kept, excl=excl, dec=dec, ht=ht, md=md, js=js, man=man)


def test_pipeline(bucket, monkeypatch, tmp_path):
    # attempt 3 gives forbidden.nl a 403 again: it stays robots_error, with the record of attempt 1
    r = run_pipeline(bucket, monkeypatch, tmp_path, {"forbidden.nl": (403, b"Error 1005")})
    t, st, kept, excl, dec, md, js, man = r.t, r.st, r.kept, r.excl, r.dec, r.md, r.js, r.man
    fb = r.ht["forbidden.nl"]
    assert fb["host_status"] == "robots_error" and fb["decided_by"] == 1 and fb["attempts"] == 2
    assert fb["robots_status"] == 403 and fb["robots_status_3"] == 403 and fb["fetched_at_3"] == THIRD_AT
    assert fb["fetched_at"] == "2026-11-14T16:00:00Z" and fb["fetched_at_2"] is None
    assert r.ht["dead.nl"]["decided_by"] == 2 and r.ht["dead.nl"]["fetched_at_3"] is None
    assert st["docs"] == t.num_rows == len(dec)
    first = {DOCS[i % len(DOCS)][0]: d["reason"] for i, d in enumerate(dec)}
    assert first == {
        "https://www.nrc.nl/nieuws/a": "robots_ai",
        "https://blog.example.nl/post": "",
        "https://blog.example.nl/admin/x": "robots_star",
        "https://blog.example.nl/post2": "crawl",
        "https://only-late.nl/x": "crawl",
        "https://dead.nl/x": "",  # unreachable: no opt-out known
        "https://forbidden.nl/x": "",  # 403 on robots.txt: no opt-out known
        "https://thepiratebay.org/x": "piracy",
        "https://tdm.nl/news/x": "tdmrep",
        "https://tdm.nl/free/x": "",
        "https://signal.nl/x": "robots_signal",
        "https://ad.nl/x": "cc_ledger",
        "https://petal.nl/x": "robots_ai_extended",
        "not a url": "",
    }
    extra = [n for n, _ in fo.OUT_EXTRA]
    assert kept.schema.names == excl.schema.names == t.schema.names + extra
    assert st["kept"] == kept.num_rows == 750 and st["excluded"] == excl.num_rows == 1050 and st["crawl"] == 300
    rows = kept.to_pylist()
    assert all(t["url"][r["source_row"]].as_py() == r["url"] for r in rows + excl.to_pylist())
    assert not any(r["optout"] or r["optout_reasons"] for r in rows)
    assert all(r["optout"] and r["optout_reasons"] for r in excl.to_pylist())
    by_url = {r["url"]: r for r in rows + excl.to_pylist()}
    assert by_url["https://dead.nl/x"]["host_status"] == "unreachable"
    assert by_url["https://dead.nl/x"]["optout_checked_at"].isoformat() == "2026-11-14T16:45:00+00:00"
    assert by_url["https://forbidden.nl/x"]["host_status"] == "robots_error"
    assert by_url["https://tdm.nl/free/x"]["host_status"] == "no_robots"
    assert by_url["not a url"]["host_status"] == "no_host" and by_url["not a url"]["optout_checked_at"] is None
    assert by_url["https://www.nrc.nl/nieuws/a"]["optout_checked_at"].isoformat() == "2026-11-14T16:00:00+00:00"
    assert by_url["https://blog.example.nl/admin/x"]["optout_reasons"] == ["robots_star"]
    assert {r["text"] for r in rows} == {
        "Een blog. Rekening NL00 BANK 0000 0000 00 graag.",
        "Dode site.",
        "403 op robots.",
        "Vrij. Kerkstraat 0, 0000 XX Utrecht.",
        "Geen host.",
    }
    assert by_url["https://www.nrc.nl/nieuws/a"]["text"] == "Nieuws. Bel 000-0000000."  # excluded/ replaced too
    assert by_url["https://www.nrc.nl/nieuws/a"]["pii_replaced"] == 1
    assert st["pii"]["iban"] == 150 and st["pii"]["address"] == 150 and st["pii"]["phone"] == 150
    assert st["pii_docs"] == 450 and st["pii_docs_kept"] == 300 and st["pii_kept"]["phone"] == 0
    assert st["only"]["robots_ai_extended"] == 150  # petal.nl: the wider list only
    assert st["host_status"] == {"ok": 900, "no_robots": 450, "robots_error": 150, "unreachable": 150, "no_host": 150}
    assert st["host_status_kept"] == {
        "ok": 150,
        "no_robots": 150,
        "robots_error": 150,
        "unreachable": 150,
        "no_host": 150,
    }
    assert sum(st["first"].values()) == st["excluded"] and st["kept"] + st["excluded"] + st["crawl"] == st["docs"]
    assert st["tokens_in"] >= st["tokens_kept"] > 0 and st["tokens_crawl"] > 0
    assert set(st["first"]) <= set(rules.REASONS)
    assert st["a3_excluded"] == {} and st["a3_excluded_hosts"] == {}

    assert (
        js["documents"]["kept"] == 750 and js["documents"]["excluded"] == 1050 and js["documents"]["in"] == t.num_rows
    )
    hs = js["hosts"]["host_status"]
    assert hs["unreachable"]["hosts"] == 1 and hs["robots_error"]["hosts"] == 1
    assert js["documents"]["host_status_kept"]["unreachable"] == 150
    assert js["documents"]["only_reason"]["robots_ai_extended"] == 150
    assert "| **behouden (`kept/`)** |" in md and "This leaves 750 documents" in md
    assert "300 documents of such sites remain" in md and "| `unreachable` | 150 | 150 | 0 |" in md
    a3 = js["attempt_3"]
    assert a3["hosts"] == 1 and a3["documents"] == 150 and a3["decided"] == {"hosts": 0, "documents": 0}
    assert a3["outcome"] == {"robots_error": {"hosts": 1, "documents": 150}}
    assert a3["detail"] == {"robots_error: HTTP 403": {"hosts": 1, "documents": 150}}
    assert a3["excluded_only_through_attempt_3"]["documents"] == 0
    assert js["snapshot_utc"]["attempt_3"] == [THIRD_AT, THIRD_AT]
    assert js["tokens"]["split_kept"] == {"train": js["tokens"]["kept"]}
    assert man["counts"]["documents_kept"] == 750 and man["counts"]["documents_excluded"] == 1050
    assert man["outputs"]["kept"][0]["rows"] == 750 and man["outputs"]["excluded"][0]["rows"] == 1050


def test_pipeline_attempt3_decides(bucket, monkeypatch, tmp_path):
    # attempt 3 gives forbidden.nl a 2xx whose robots.txt disallows GPTBot: it becomes ok and its document is excluded
    r = run_pipeline(bucket, monkeypatch, tmp_path, {"forbidden.nl": (200, b"User-agent: GPTBot\nDisallow: /\n")})
    fb = r.ht["forbidden.nl"]
    assert fb["host_status"] == "ok" and fb["decided_by"] == 3 and fb["robots_status"] == 200
    assert fb["fetched_at"] == THIRD_AT and fb["fetched_at_1"] == "2026-11-14T16:00:00Z"
    assert fb["robots_url"] == "https://forbidden.nl/robots.txt" and fb["root_blocked"] == '["gptbot"]'
    by_url = {x["url"]: x for x in r.kept.to_pylist() + r.excl.to_pylist()}
    doc = by_url["https://forbidden.nl/x"]
    assert doc["optout"] and doc["optout_reasons"] == ["robots_ai"] and doc["host_status"] == "ok"
    assert doc["optout_checked_at"].isoformat() == "2026-11-15T15:00:00+00:00"
    st = r.st
    assert st["kept"] == 600 and st["excluded"] == 1200
    assert st["a3_excluded"] == {"robots_ai": 150} and st["a3_excluded_hosts"]["forbidden.nl"][0] == 150
    assert st["a3_excluded_tokens"]["robots_ai"] == st["a3_excluded_hosts"]["forbidden.nl"][1] > 0
    assert "robots_error" not in st["host_status"] and st["host_status"]["ok"] == 1050
    a3 = r.js["attempt_3"]
    assert a3["outcome"] == {"ok": {"hosts": 1, "documents": 150}} and a3["decided"] == {"hosts": 1, "documents": 150}
    x3 = a3["excluded_only_through_attempt_3"]
    assert x3["documents"] == 150 and x3["first_reason"] == {"robots_ai": 150} and x3["hosts"] == 1
    assert x3["top_hosts"][0][:2] == ["forbidden.nl", 150]
    assert "| 2xx: wordt `ok`, regels gelezen | 1 | 150 |" in r.md and "| robots_ai | 150 |" in r.md
    assert r.js["documents"]["kept"] == 600 and "This leaves 600 documents" in r.md
    assert r.js["attempt_3"]["skipped"]["hosts"] == 0 and "Niet opnieuw gevraagd" not in r.md


def test_pipeline_attempt3_skipped(bucket, monkeypatch, tmp_path):
    # attempt 3 is stopped before the unit of forbidden.nl is done: on fetch3-skipped.parquet it keeps attempt 1
    r = run_pipeline(bucket, monkeypatch, tmp_path, None)
    fb = r.ht["forbidden.nl"]
    assert fb["host_status"] == "robots_error" and fb["decided_by"] == 1 and fb["attempts"] == 1
    assert fb["fetched_at_3"] is None and fb["robots_status_3"] is None and fb["robots_status"] == 403
    by_url = {x["url"]: x for x in r.kept.to_pylist() + r.excl.to_pylist()}
    doc = by_url["https://forbidden.nl/x"]
    assert not doc["optout"] and doc["host_status"] == "robots_error"
    assert doc["optout_checked_at"].isoformat() == "2026-11-14T16:00:00+00:00"
    assert r.st["kept"] == 750 and r.st["excluded"] == 1050 and r.st["a3_excluded"] == {}
    a3 = r.js["attempt_3"]
    assert a3["hosts"] == 0 and a3["outcome"] == {} and r.js["snapshot_utc"]["attempt_3"] is None
    assert a3["skipped"] == {
        "hosts": 1,
        "documents": 150,
        "units": ["u0000"],
        "top_regdoms": [["forbidden.nl", 1, 150]],
        "list": "gs://b/r/work/fetch3-skipped.parquet",
    }
    assert "**Niet opnieuw gevraagd:** 1 hosts (150 documenten t/m de grens) in 1 units" in r.md
    assert "forbidden.nl (1, 150)" in r.md and "which was not done for 1 hosts" in r.md


def test_table_needs_attempt3(bucket, monkeypatch, tmp_path):
    # a robots_error host without an attempt-3 record stops the table
    import fw2optout as fo2

    monkeypatch.setattr(fo2, "_LISTS", None)
    bucket["gs://b/r/lists/apertus-fw2_domains.txt"] = b"nothing.example\n"
    bucket["gs://b/r/lists/cc-optout-ledger.csv"] = b"a,b,c\nLAST UPDATED,,\n,,\n"
    bucket["gs://b/r/lists/piracy_domains.tsv"] = (fo2.HERE / "lists" / "piracy_domains.tsv").read_bytes()
    bucket["gs://b/r/work/fetch-shards/index.json"] = json.dumps({"shards": [{"name": "s0"}]}).encode()
    rec = {f.name: None for f in fo2.RAW_SCHEMA}
    rec.update(host="a.nl", regdom="a.nl", n_le=1, attempt=1, fetched_at="2026-11-14T16:00:00Z", robots_status=403)
    bucket["gs://b/r/work/fetch1/s0.parquet"] = fo2.table_bytes(pa.Table.from_pylist([rec], schema=fo2.RAW_SCHEMA))
    bucket["gs://b/r/work/fetch2/s0.parquet"] = fo2.table_bytes(fo2.RAW_SCHEMA.empty_table())
    with pytest.raises(SystemExit, match="attempt 3 is not planned"):
        fo2.cmd_table(types.SimpleNamespace(root="gs://b/r", processes=1))
    fo2.cmd_hosts3(types.SimpleNamespace(root="gs://b/r", processes=1))
    with pytest.raises(SystemExit, match="attempt 3 not complete"):
        fo2.cmd_table(types.SimpleNamespace(root="gs://b/r", processes=1))
    with pytest.raises(RuntimeError, match="exactly the hosts with robots_error"):
        fo2.table_shard(("gs://b/r", "s0", {}, set()))


def test_table_skipped(bucket, monkeypatch, tmp_path):
    # only a host on fetch3-skipped.parquet may lack attempt 3; the list must be exactly the units without a result
    import fw2optout as fo2

    monkeypatch.setattr(fo2, "_LISTS", None)
    bucket["gs://b/r/lists/apertus-fw2_domains.txt"] = b"nothing.example\n"
    bucket["gs://b/r/lists/cc-optout-ledger.csv"] = b"a,b,c\nLAST UPDATED,,\n,,\n"
    bucket["gs://b/r/lists/piracy_domains.tsv"] = (fo2.HERE / "lists" / "piracy_domains.tsv").read_bytes()
    bucket["gs://b/r/work/fetch-shards/index.json"] = json.dumps({"shards": [{"name": "s0"}]}).encode()
    recs = []
    for h in ("a.nl", "b.nl"):
        rec = {f.name: None for f in fo2.RAW_SCHEMA}
        rec.update(host=h, regdom=h, n_le=1, attempt=1, fetched_at="2026-11-14T16:00:00Z", robots_status=403)
        recs.append(rec)
    bucket["gs://b/r/work/fetch1/s0.parquet"] = fo2.table_bytes(pa.Table.from_pylist(recs, schema=fo2.RAW_SCHEMA))
    bucket["gs://b/r/work/fetch2/s0.parquet"] = fo2.table_bytes(fo2.RAW_SCHEMA.empty_table())
    args = types.SimpleNamespace(root="gs://b/r", processes=1)
    fo2.cmd_hosts3(args)
    fo2.cmd_skip3(args)
    sk = pq.read_table(io.BytesIO(bucket["gs://b/r/work/fetch3-skipped.parquet"])).to_pylist()
    assert [(x["unit"], x["host"]) for x in sk] == [("u0000", "a.nl"), ("u0000", "b.nl")]
    with pytest.raises(SystemExit, match="exists"):
        fo2.cmd_skip3(args)
    t = fo2.table_shard(("gs://b/r", "s0", {}, {"a.nl", "b.nl"})).to_pylist()
    assert [(x["host"], x["host_status"], x["decided_by"], x["fetched_at_3"]) for x in t] == [
        ("a.nl", "robots_error", 1, None),
        ("b.nl", "robots_error", 1, None),
    ]
    with pytest.raises(RuntimeError, match="exactly the hosts with robots_error"):
        fo2.table_shard(("gs://b/r", "s0", {}, {"a.nl"}))
    with pytest.raises(RuntimeError, match="hosts not in it"):
        fo2.table_shard(("gs://b/r", "s0", {}, {"a.nl", "b.nl", "c.nl"}))
    rec3 = {f.name: None for f in fo2.RAW3_SCHEMA}
    rec3.update(shard="s0", host="a.nl", fetched_at="2026-11-15T15:00:00Z", robots_status=403)
    with pytest.raises(RuntimeError, match="in fetch3-skipped.parquet, so robots_error without attempt 3"):
        fo2.table_shard(("gs://b/r", "s0", {"a.nl": rec3}, {"a.nl", "b.nl"}))
    fo2.cmd_table(args)
    ht = pq.read_table(io.BytesIO(bucket["gs://b/r/optout/hosts.parquet"])).to_pylist()
    assert [(x["host"], x["host_status"], x["attempts"]) for x in ht] == [
        ("a.nl", "robots_error", 1),
        ("b.nl", "robots_error", 1),
    ]
    # a unit on the list that has a result after all, or a list that misses a host of its unit, stops the table
    bucket["gs://b/r/work/fetch3/u0000.parquet"] = fo2.table_bytes(pa.Table.from_pylist([rec3], schema=fo2.RAW3_SCHEMA))
    with pytest.raises(SystemExit, match="units that have a result"):
        fo2.cmd_table(args)
    del bucket["gs://b/r/work/fetch3/u0000.parquet"]
    bucket["gs://b/r/work/fetch3-skipped.parquet"] = fo2.table_bytes(pa.Table.from_pylist(sk[:1]))
    with pytest.raises(SystemExit, match="the hosts differ"):
        fo2.cmd_table(args)
