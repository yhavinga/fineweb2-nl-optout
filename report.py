"""The report (Markdown and JSON) and the manifest of a finished run: the counts of every step, from the bucket."""

from __future__ import annotations

import io
import json
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pyarrow.parquet as pq

import gcs
import pii
import rules
from fw2optout import OUT_SETS, stats_uri

HERE = Path(__file__).resolve().parent


def _json(uri):
    return json.loads(gcs.read_bytes(uri))


def _sum(stats: list[dict], key: str) -> Counter:
    c: Counter = Counter()
    for s in stats:
        c.update(s.get(key, {}))
    return c


def pct(a, b) -> str:
    return f"{100 * a / b:.2f} %" if b else "–"


def fmt(n) -> str:
    return f"{n:,}".replace(",", ".")


COLUMNS = {
    "source_file": "the FineWeb-2 file of the document",
    "source_row": "its row in that file",
    "optout": "true when any reason of the rule holds (the document is in excluded/), false otherwise (in kept/)",
    "optout_reasons": "every reason that holds, in the order of the rule (empty when kept)",
    "host_status": "what the host's robots.txt fetch gave: " + ", ".join(rules.HOST_STATUSES),
    "optout_checked_at": "UTC time of the fetch of the host's robots.txt that decided (the second attempt for a host "
    "that was unreachable at first; the third, from outside Google Cloud, for a host whose robots.txt gave an error "
    "and then a 2xx, 404 or 410); null without host",
    "pii_replaced": "number of personal-data replacements in the text (pii.py)",
}


def run(args):
    root = args.root
    w = f"{root}/work"
    plan = _json(f"{w}/plan.json")
    names = [u["name"] for u in plan["filter_units"]]
    sdir = stats_uri(root, args.run)
    with ThreadPoolExecutor(args.processes) as ex:
        stats = list(ex.map(lambda n: _json(f"{sdir}/{n}.json"), names))
        hosts_units = list(ex.map(lambda u: _json(f"{w}/hosts/{u['name']}.json"), plan["host_units"]))

        def out_rows(uri):
            b = gcs.blob(uri)
            b.reload()
            with b.open("rb", chunk_size=1 << 20) as fh:  # only the footer is read
                return pq.ParquetFile(fh).metadata.num_rows, b.crc32c

        rows_out = {k: list(ex.map(out_rows, [s["out"][k]["uri"] for s in stats])) for k in OUT_SETS}
    for k in OUT_SETS:
        bad = [(s["unit"], s[k], r) for s, (r, _) in zip(stats, rows_out[k], strict=True) if s[k] != r]
        if bad:
            raise SystemExit(f"rows of {k}/ differ from the stats: {bad[:5]}")
        # the file is the one this filter run wrote, not one of an earlier run
        bad = [s["unit"] for s, (_, c) in zip(stats, rows_out[k], strict=True) if s["out"][k]["crc32c"] != c]
        if bad:
            raise SystemExit(f"{k}/ files differ from those of filter run {args.run}: {bad[:5]}")
    bad = [s["unit"] for s in stats if s["kept"] + s["excluded"] + s["crawl"] != s["docs"]]
    if bad:
        raise SystemExit(f"kept + excluded + crawl != documents in {bad[:5]}")
    lists_manifest = _json(f"{root}/lists/manifest.json")
    shards_index = _json(f"{w}/fetch-shards/index.json")
    ht = pq.read_table(io.BytesIO(gcs.read_bytes(f"{root}/optout/hosts.parquet")))

    # ---------------------------------------------------------------- documents
    docs = sum(s["docs"] for s in stats)
    crawl = sum(s["crawl"] for s in stats)
    in_limit = sum(s["in_limit"] for s in stats)
    kept = sum(s["kept"] for s in stats)
    excluded = sum(s["excluded"] for s in stats)
    first, anyr, only = _sum(stats, "first"), _sum(stats, "any"), _sum(stats, "only")
    tok_first = _sum(stats, "tokens_first")
    tok_in = sum(s["tokens_in"] for s in stats)
    tok_crawl = sum(s["tokens_crawl"] for s in stats)
    tok_kept = sum(s["tokens_kept"] for s in stats)
    chars_in = sum(s["chars_in"] for s in stats)
    chars_crawl = sum(s["chars_crawl"] for s in stats)
    chars_kept = sum(s["chars_kept"] for s in stats)
    status_docs, status_kept = _sum(stats, "host_status"), _sum(stats, "host_status_kept")
    piic, piic_kept = _sum(stats, "pii"), _sum(stats, "pii_kept")
    pii_docs = sum(s["pii_docs"] for s in stats)
    pii_docs_kept = sum(s["pii_docs_kept"] for s in stats)
    split_rows, split_kept, split_excl, split_tok_kept = Counter(), Counter(), Counter(), Counter()
    for u, s in zip(plan["filter_units"], stats, strict=True):
        split_rows[u["split"]] += s["docs"]
        split_kept[u["split"]] += s["kept"]
        split_excl[u["split"]] += s["excluded"]
        split_tok_kept[u["split"]] += s["tokens_kept"]
    a3_first, a3_tok = _sum(stats, "a3_excluded"), _sum(stats, "a3_excluded_tokens")
    a3_hosts: Counter = Counter()
    a3_hosts_tok: Counter = Counter()
    for s in stats:
        for host, (nd, nt) in s["a3_excluded_hosts"].items():
            a3_hosts[host] += nd
            a3_hosts_tok[host] += nt
    dumps = Counter()
    for h in hosts_units:
        dumps.update(h["dumps"])
    if sum(dumps.values()) != docs:
        raise SystemExit(f"documents in the hosts phase {sum(dumps.values())} != filter phase {docs}")

    # ---------------------------------------------------------------- hosts
    n_le = ht["n_le"].to_pylist()
    hstatus = ht["host_status"].to_pylist()
    err = ht["robots_error"].to_pylist()
    status = ht["robots_status"].to_pylist()
    host_rows = ht.num_rows

    def hcount(mask) -> tuple[int, int]:
        return sum(1 for m in mask if m), sum(n for n, m in zip(n_le, mask, strict=True) if m)

    status_counts = {
        o: hcount([x == o for x in hstatus]) for o in (rules.OK, rules.ABSENT, rules.ERROR, rules.UNREACHABLE)
    }
    unreach_detail = Counter()
    for o, e, s in zip(hstatus, err, status, strict=True):
        if o == rules.UNREACHABLE:
            unreach_detail[e or f"HTTP {s}"] += 1
    error_detail = Counter(f"HTTP {s}" for o, s in zip(hstatus, status, strict=True) if o == rules.ERROR)
    rb = [json.loads(x) if x else [] for x in ht["root_blocked"].to_pylist()]
    core, ext = rules.agent_tokens(HERE)
    flags = {
        "robots.txt: / verboden voor *": [("*" in r) for r in rb],
        "robots.txt: / verboden voor een agent van de kernlijst": [any(t in core for t in r) for r in rb],
        "robots.txt: / verboden alleen voor agents van de ai.robots.txt-lijst": [
            bool(r) and "*" not in r and not any(t in core for t in r) for r in rb
        ],
        "robots.txt: regels (Disallow) voor * of een agent van de lijst": [
            bool(x) for x in ht["robots_rules"].to_pylist()
        ],
        "Content-Signal ai-train=no / Content-Usage": [
            bool(a) or bool(b) for a, b in zip(ht["signal"].to_pylist(), ht["usage"].to_pylist(), strict=True)
        ],
        "tdmrep.json aanwezig en geldig": ht["tdm_valid"].to_pylist(),
        "tdmrep.json met tdm-reservation 1": [bool(x) for x in ht["tdm_rules"].to_pylist()],
        "ai.txt met regels": ht["ai_valid"].to_pylist(),
        "ai.txt die tekst voorbehoudt": ht["ai_text"].to_pylist(),
        "Apertus-bloklijst": ht["apertus"].to_pylist(),
        "Common Crawl Opt-Out Ledger (domein)": ht["ledger"].to_pylist(),
        "Common Crawl Opt-Out Ledger (URL-prefix)": [bool(x) for x in ht["ledger_paths"].to_pylist()],
        "piraterijlijst": ht["piracy"].to_pylist(),
    }
    flag_counts = {k: hcount(v) for k, v in flags.items()}
    agent_hits = Counter(t for r in rb for t in r)
    window = {}
    for a in (1, 2, 3):
        x = [v for v in ht[f"fetched_at_{a}"].to_pylist() if v]
        window[f"attempt_{a}"] = [min(x), max(x)] if x else None

    # attempt 3: the robots.txt of the robots_error hosts again, from outside Google Cloud
    a3 = {"outcome": Counter(), "outcome_docs": Counter(), "detail": Counter(), "detail_docs": Counter()}
    for f3, s3, e3, n in zip(
        ht["fetched_at_3"].to_pylist(),
        ht["robots_status_3"].to_pylist(),
        ht["robots_error_3"].to_pylist(),
        n_le,
        strict=True,
    ):
        if f3 is None:
            continue
        o = rules.robots_outcome(s3, e3)
        a3["outcome"][o] += 1
        a3["outcome_docs"][o] += n
        key = f"{o}: " + (e3 if e3 else f"HTTP {s3}")
        a3["detail"][key] += 1
        a3["detail_docs"][key] += n
    decided3 = hcount([x == 3 for x in ht["decided_by"].to_pylist()])
    # the hosts of the units of attempt 3 that were stopped without a result: they keep robots_error (rules.py)
    sk_uri = f"{w}/fetch3-skipped.parquet"
    sk = pq.read_table(io.BytesIO(gcs.read_bytes(sk_uri))) if gcs.exists(sk_uri) else None
    sk_hosts = set(sk["host"].to_pylist()) if sk is not None else set()
    no3 = {
        x
        for x, o, f3 in zip(ht["host"].to_pylist(), hstatus, ht["fetched_at_3"].to_pylist(), strict=True)
        if o == rules.ERROR and f3 is None
    }
    if no3 != sk_hosts or (sk is not None and sk.num_rows != len(sk_hosts)):
        raise SystemExit(
            f"robots_error hosts without attempt 3 ({len(no3)}) are not those of {sk_uri} ({len(sk_hosts)})"
        )
    skipped3 = {"hosts": 0, "documents": 0, "units": [], "top_regdoms": [], "list": None}
    if sk is not None:
        by_regdom: dict[str, list[int]] = {}
        for rd, n in zip(sk["regdom"].to_pylist(), sk["n_le"].to_pylist(), strict=True):
            v = by_regdom.setdefault(rd, [0, 0])
            v[0] += 1
            v[1] += n
        skipped3 = {
            "hosts": sk.num_rows,
            "documents": sum(sk["n_le"].to_pylist()),
            "units": sorted(set(sk["unit"].to_pylist())),
            "top_regdoms": [[rd, nh, nd] for rd, (nh, nd) in sorted(by_regdom.items(), key=lambda x: -x[1][1])[:25]],
            "list": sk_uri,
        }

    def top(mask, k=15):
        rows = sorted(((n, h) for n, h, m in zip(n_le, ht["host"].to_pylist(), mask, strict=True) if m), reverse=True)
        return [(h, n) for n, h in rows[:k]]

    tops = {
        "robots.txt: / verboden voor * of een agent van de lijst": top([bool(r) for r in rb]),
        "robots_error (blijven, tenzij een andere reden geldt)": top([o == rules.ERROR for o in hstatus]),
        "unreachable (blijven, tenzij een andere reden geldt)": top([o == rules.UNREACHABLE for o in hstatus]),
        "Common Crawl Opt-Out Ledger": top(flags["Common Crawl Opt-Out Ledger (domein)"]),
        "piraterijlijst": top(flags["piraterijlijst"]),
        "tdmrep.json met voorbehoud": top(flags["tdmrep.json met tdm-reservation 1"]),
    }

    # ---------------------------------------------------------------- manifest
    outputs = {
        k: [
            {
                "file": s["out"][k]["uri"].split(f"{root}/", 1)[1],
                "rows": s[k],
                "bytes": s["out"][k]["bytes"],
                "crc32c": s["out"][k]["crc32c"],
                "md5": s["out"][k]["md5"],
                "source_file": s["path"],
                "row_groups": s["rg"],
                "source_etag": s["etag"],
                "code": s["code"],
            }
            for s in stats
        ]
        for k in OUT_SETS
    }
    manifest = {
        "dataset": "fineweb2-nl-optout",
        "source": {
            "repo": plan["repo"],
            "revision": plan["revision"],
            "config": plan["config"],
            "files": plan["files"],
        },
        "code": {
            "repository": rules.REPO,
            "report_commit": args.commit,
            "filter_code": sorted({s["code"] for s in stats}),
        },
        "rule": rules.__doc__.strip(),
        "crawl_last": rules.CRAWL_LAST,
        "reasons": rules.REASONS,
        "host_statuses": rules.HOST_STATUSES,
        "columns": COLUMNS,
        "user_agent": rules.USER_AGENT,
        "agents": {
            "core": sorted(core),
            "extended_from": rules.EXTENDED_SOURCE,
            "extended_commit": rules.EXTENDED_COMMIT,
            "extended": sorted(ext),
        },
        "optout_snapshot_utc": window,
        "lists": lists_manifest["files"],
        "pii": {"types": pii.TYPES, "placeholders": pii.PLACEHOLDER},
        "tokenizer": "yhavinga/dutch-llama-tokenizer@d6a3a55a4ddf51a05d98d6a07f170f61a3c7c1ff",
        "counts": {
            "documents_in": docs,
            "documents_after_crawl_limit": in_limit,
            "documents_kept": kept,
            "documents_excluded": excluded,
            "tokens_in": tok_in,
            "tokens_kept": tok_kept,
            "split_rows": dict(split_rows),
            "split_kept": dict(split_kept),
            "split_excluded": dict(split_excl),
            "split_tokens_kept": dict(split_tok_kept),
        },
        "outputs": {
            "kept": outputs["kept"],
            "excluded": outputs["excluded"],
            "excluded_use": "internal use and analysis only; a published version contains kept/ only",
            "decisions": f"{root}/decisions/",
            "hosts": f"{root}/optout/hosts.parquet",
        },
    }
    gcs.write_bytes(
        f"{root}/manifest.json", json.dumps(manifest, ensure_ascii=False, indent=1).encode(), "application/json"
    )

    # ---------------------------------------------------------------- report
    summary = {
        "documents": {
            "in": docs,
            "crawl": crawl,
            "after_crawl_limit": in_limit,
            "kept": kept,
            "excluded": excluded,
            "first_reason": dict(first),
            "any_reason": dict(anyr),
            "only_reason": dict(only),
            "host_status": dict(status_docs),
            "host_status_kept": dict(status_kept),
            "split_in": dict(split_rows),
            "split_kept": dict(split_kept),
            "split_excluded": dict(split_excl),
        },
        "tokens": {
            "in": tok_in,
            "crawl": tok_crawl,
            "after_crawl_limit": tok_in - tok_crawl,
            "kept": tok_kept,
            "split_kept": dict(split_tok_kept),
            "first_reason": dict(tok_first),
            "note": "documents up to the crawl limit counted after the PII replacement; "
            "tokenizer yhavinga/dutch-llama-tokenizer",
        },
        "characters": {"in": chars_in, "crawl": chars_crawl, "kept": chars_kept},
        "dumps": dict(sorted(dumps.items())),
        "hosts": {
            "total_with_document_up_to_limit": host_rows,
            "fetch_shards": len(shards_index["shards"]),
            "host_status": {k: {"hosts": v[0], "documents": v[1]} for k, v in status_counts.items()},
            "unreachable_detail": dict(unreach_detail),
            "robots_error_detail": dict(error_detail),
            "flags": {k: {"hosts": v[0], "documents": v[1]} for k, v in flag_counts.items()},
            "root_blocked_agents": dict(agent_hits.most_common()),
            "top": tops,
        },
        "attempt_3": {
            "hosts": sum(a3["outcome"].values()),
            "documents": sum(a3["outcome_docs"].values()),
            "outcome": {o: {"hosts": a3["outcome"][o], "documents": a3["outcome_docs"][o]} for o in a3["outcome"]},
            "detail": {k: {"hosts": v, "documents": a3["detail_docs"][k]} for k, v in a3["detail"].most_common()},
            "decided": {"hosts": decided3[0], "documents": decided3[1]},
            "skipped": skipped3,
            "excluded_only_through_attempt_3": {
                "documents": sum(a3_first.values()),
                "tokens": sum(a3_tok.values()),
                "first_reason": dict(a3_first),
                "tokens_first_reason": dict(a3_tok),
                "top_hosts": [[h, n, a3_hosts_tok[h]] for h, n in a3_hosts.most_common(25)],
                "hosts": len(a3_hosts),
            },
        },
        "pii": {
            "replacements": dict(piic),
            "documents_with_replacement": pii_docs,
            "replacements_kept": dict(piic_kept),
            "documents_with_replacement_kept": pii_docs_kept,
        },
        "columns": COLUMNS,
        "snapshot_utc": window,
        "lists": lists_manifest["files"],
        "agents": manifest["agents"],
        "manifest": f"{root}/manifest.json",
    }
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    (out / "fineweb2-nl-optout.json").write_text(json.dumps(summary, ensure_ascii=False, indent=1))
    (out / "fineweb2-nl-optout.md").write_text(markdown(summary, manifest, args.commit))
    print("REPORT", out / "fineweb2-nl-optout.md")


def markdown(s: dict, m: dict, commit: str) -> str:
    d, t, h = s["documents"], s["tokens"], s["hosts"]
    L = []
    a = L.append
    a("# FineWeb-2 nld_Latn, gefilterd op opt-outs: rapport")
    a("")
    a(
        f"Gemaakt door `report.py` van [fineweb2-nl-optout]({rules.REPO}) @ `{commit}`; filtercode "
        f"{', '.join('`' + c + '`' for c in m['code']['filter_code'])}. Alle getallen zijn tellingen van deze run "
        f"**[eigen meting]**. Bron: `{m['source']['repo']}` @ `{m['source']['revision']}`, config "
        f"`{m['source']['config']}`. Uitvoer: `{m['outputs']['decisions'].rsplit('/decisions/', 1)[0]}/`."
    )
    a("")
    w = s["snapshot_utc"]
    a(
        f"**Opt-out-momentopname (UTC):** eerste poging {w['attempt_1'][0]} – {w['attempt_1'][1]}"
        + (
            f"; tweede poging (alleen eerst onbereikbare hosts) {w['attempt_2'][0]} – {w['attempt_2'][1]}"
            if w["attempt_2"]
            else ""
        )
        + (
            f"; derde poging (alleen robots.txt van hosts met `robots_error`, vanaf een server buiten Google Cloud) "
            f"{w['attempt_3'][0]} – {w['attempt_3'][1]}."
            if w["attempt_3"]
            else "."
        )
    )
    a("")
    a("## Uitvoer")
    a("")
    a(
        f"Elk document t/m `{rules.CRAWL_LAST}` staat in de uitvoer, met zijn beslissing in extra kolommen: "
        f"`kept/{{train,test}}/` ({fmt(d['kept'])} documenten, `optout` = false) en `excluded/{{train,test}}/` "
        f"({fmt(d['excluded'])} documenten, `optout` = true), met hetzelfde schema. De trainingsset is `kept/`. "
        "`excluded/` is alleen voor intern gebruik en analyse; een publicatie bevat alleen `kept/`. Kolommen bovenop "
        "die van FineWeb-2:"
    )
    a("")
    a("| kolom | betekenis |")
    a("|---|---|")
    for k, v in s["columns"].items():
        a(f"| `{k}` | {v} |")
    a("")
    a("## Documenten per stap")
    a("")
    a(
        "Elk uitgesloten document telt onder de eerste reden in deze volgorde (`rules.REASONS`). Tokens met "
        "`yhavinga/dutch-llama-tokenizer`; documenten t/m de crawljaargrens na de PII-vervanging geteld."
    )
    a("")
    a("| stap | documenten weg | % van alle | tokens weg | over |")
    a("|---|---:|---:|---:|---:|")
    left = d["in"]
    a(f"| FineWeb-2 nld_Latn (train + test) | | | | {fmt(left)} docs, {fmt(t['in'])} tokens |")
    left -= d["crawl"]
    a(
        f"| crawl na `{rules.CRAWL_LAST}` (niet in de uitvoer) | {fmt(d['crawl'])} | {pct(d['crawl'], d['in'])} | "
        f"{fmt(t['crawl'])} | {fmt(left)} |"
    )
    for r in rules.REASONS:
        n = d["first_reason"].get(r, 0)
        left -= n
        a(f"| {r} | {fmt(n)} | {pct(n, d['in'])} | {fmt(t['first_reason'].get(r, 0))} | {fmt(left)} |")
    a(
        f"| **behouden (`kept/`)** | | | | **{fmt(d['kept'])} docs ({pct(d['kept'], d['in'])}), "
        f"{fmt(t['kept'])} tokens** |"
    )
    a("")
    a(
        f"Na de crawljaargrens (t/m `{rules.CRAWL_LAST}`): {fmt(d['after_crawl_limit'])} documenten, "
        f"{fmt(t['after_crawl_limit'])} tokens. Daarvan behouden: {pct(d['kept'], d['after_crawl_limit'])} van de "
        f"documenten en {pct(t['kept'], t['after_crawl_limit'])} van de tokens. Per split (behouden / uitgesloten / "
        "alle): "
        + ", ".join(
            f"{k} {fmt(d['split_kept'].get(k, 0))} / {fmt(d['split_excluded'].get(k, 0))} / {fmt(v)}"
            for k, v in d["split_in"].items()
        )
        + ". Tokens in `kept/` per split: "
        + ", ".join(f"{k} {fmt(v)}" for k, v in t["split_kept"].items())
        + "."
    )
    a("")
    a(
        "Elke reden op zichzelf (een document kan onder meerdere vallen), en het aantal documenten waarvoor het de "
        "enige reden is (wat die reden alleen kost):"
    )
    a("")
    a("| reden | documenten (alle) | % na crawlgrens | enige reden |")
    a("|---|---:|---:|---:|")
    for r in rules.REASONS:
        n = d["any_reason"].get(r, 0)
        a(f"| {r} | {fmt(n)} | {pct(n, d['after_crawl_limit'])} | {fmt(d['only_reason'].get(r, 0))} |")
    a("")
    a(
        f"Alleen door de ruimere ai.robots.txt-lijst (`robots_ai_extended` als enige reden): "
        f"{fmt(d['only_reason'].get('robots_ai_extended', 0))} documenten. Dat zijn documenten waarvan het pad "
        "uitsluitend verboden is voor een agent van die lijst buiten de kernlijst (bijvoorbeeld PetalBot of Exabot, "
        "die ook als zoekmachine bekend zijn)."
    )
    a("")
    a("## Hosts zonder bruikbare robots.txt")
    a("")
    a(
        "Een host zonder robots.txt (`no_robots`), met een fout op robots.txt (`robots_error`) of onbereikbaar "
        "(`unreachable`) heeft geen bekend voorbehoud in robots.txt; zijn documenten blijven, tenzij een andere reden "
        "geldt (tdmrep.json, ai.txt, de lijsten). Documenten t/m de crawljaargrens naar `host_status`:"
    )
    a("")
    a("| host_status | documenten | waarvan behouden | waarvan uitgesloten (andere reden) |")
    a("|---|---:|---:|---:|")
    for k in rules.HOST_STATUSES:
        n, kk = d["host_status"].get(k, 0), d["host_status_kept"].get(k, 0)
        if n or k != rules.NO_HOST:
            a(f"| `{k}` | {fmt(n)} | {fmt(kk)} | {fmt(n - kk)} |")
    a("")
    a3 = s["attempt_3"]
    x3 = a3["excluded_only_through_attempt_3"]
    sk3 = a3["skipped"]
    a("## Derde poging: robots.txt van `robots_error`-hosts vanaf een server buiten Google Cloud")
    a("")
    a(
        f"De {fmt(a3['hosts'])} hosts ({fmt(a3['documents'])} documenten t/m de grens) waarvan robots.txt in de eerste "
        "twee pogingen (vanuit Google Cloud) een 4xx behalve 404/410/429 gaf, zijn opnieuw gevraagd vanaf een server "
        "buiten Google Cloud, met dezelfde user-agent, parser en 429-behandeling; alleen robots.txt (tdmrep.json en "
        "ai.txt blijven die van eerder). Een 2xx maakt de host `ok` met de regels van dat robots.txt, een 404/410 "
        "`no_robots`; een andere 4xx of geen bruikbaar antwoord laat hem `robots_error`."
    )
    a("")
    if sk3["hosts"]:
        a(
            f"**Niet opnieuw gevraagd:** {fmt(sk3['hosts'])} hosts ({fmt(sk3['documents'])} documenten t/m de "
            f"grens) in {len(sk3['units'])} units van de derde poging die bij het stoppen ervan geen resultaat hadden "
            "(`work/fetch3-skipped.parquet`). Ze houden het record van poging 1/2 en blijven `robots_error`, zonder "
            "bekend voorbehoud (de regel hieronder). Grootste geregistreerde domeinen (hosts, documenten): "
            + ", ".join(f"{rd} ({fmt(nh)}, {fmt(nd)})" for rd, nh, nd in sk3["top_regdoms"][:15])
            + "."
        )
        a("")
    a("| uitkomst derde poging | hosts | documenten (t/m grens) |")
    a("|---|---:|---:|")
    o3 = {
        rules.OK: "2xx: wordt `ok`, regels gelezen",
        rules.ABSENT: "404/410: wordt `no_robots`",
        rules.ERROR: "andere 4xx: blijft `robots_error`",
        rules.UNREACHABLE: "onbereikbaar, 429 of 5xx: blijft `robots_error`",
    }
    for k, label in o3.items():
        v = a3["outcome"].get(k, {"hosts": 0, "documents": 0})
        a(f"| {label} | {fmt(v['hosts'])} | {fmt(v['documents'])} |")
    a("")
    a(
        "Naar antwoord (hosts / documenten): "
        + ", ".join(f"{k} {fmt(v['hosts'])} / {fmt(v['documents'])}" for k, v in list(a3["detail"].items())[:30])
        + "."
    )
    a("")
    a(
        f"Door de derde poging uitgesloten (zonder die poging behouden: alle redenen komen uit het robots.txt van de "
        f"derde poging): {fmt(x3['documents'])} documenten, {fmt(x3['tokens'])} tokens, van {fmt(x3['hosts'])} hosts."
    )
    a("")
    a("| eerste reden | documenten | tokens |")
    a("|---|---:|---:|")
    for r in rules.ROBOTS_REASONS:
        a(f"| {r} | {fmt(x3['first_reason'].get(r, 0))} | {fmt(x3['tokens_first_reason'].get(r, 0))} |")
    a("")
    if x3["top_hosts"]:
        a(
            "Grootste hosts daarvan (documenten, tokens): "
            + ", ".join(f"{hh} ({fmt(n)}, {fmt(tk)})" for hh, n, tk in x3["top_hosts"][:15])
            + "."
        )
        a("")
    a("## Hosts")
    a("")
    a(
        f"{fmt(h['total_with_document_up_to_limit'])} hosts met minstens één document t/m de crawljaargrens, in "
        f"{fmt(h['fetch_shards'])} fetch-shards. Per host: `robots.txt`, `/.well-known/tdmrep.json`, `/ai.txt`, "
        f"met user-agent `{m['user_agent']}`."
    )
    a("")
    a("| robots.txt (`host_status`) | hosts | documenten (t/m grens) |")
    a("|---|---:|---:|")
    names = {
        rules.OK: "2xx: regels gelezen",
        rules.ABSENT: "404/410: geen robots.txt, geen beperking",
        rules.ERROR: "4xx behalve 404/410/429, ook in de derde poging geen 2xx/404/410 (of niet opnieuw gevraagd): "
        "geen voorbehoud bekend",
        rules.UNREACHABLE: "onbereikbaar, 429 of 5xx na 2 pogingen: geen voorbehoud bekend",
    }
    for k, v in h["host_status"].items():
        a(f"| `{k}`: {names[k]} | {fmt(v['hosts'])} | {fmt(v['documents'])} |")
    a("")
    a(
        "`unreachable` naar oorzaak (hosts): "
        + ", ".join(f"{k} {fmt(v)}" for k, v in sorted(h["unreachable_detail"].items(), key=lambda x: -x[1]))
        + "."
    )
    a(
        "`robots_error` naar status (hosts): "
        + ", ".join(f"{k} {fmt(v)}" for k, v in sorted(h["robots_error_detail"].items(), key=lambda x: -x[1]))
        + "."
    )
    a("")
    a("| kenmerk van de host | hosts | documenten (t/m grens) |")
    a("|---|---:|---:|")
    for k, v in h["flags"].items():
        a(f"| {k} | {fmt(v['hosts'])} | {fmt(v['documents'])} |")
    a("")
    a(
        "Agents waarvoor `/` het vaakst verboden is (hosts): "
        + ", ".join(f"`{k}` {fmt(v)}" for k, v in list(h["root_blocked_agents"].items())[:25])
        + "."
    )
    a("")
    for k, rows in h["top"].items():
        if rows:
            a(f"Grootste hosts, {k}: " + ", ".join(f"{x} ({fmt(n)})" for x, n in rows) + ".")
            a("")
    a("## Persoonsgegevens")
    a("")
    a(
        "Bovenop de anonimisering van e-mail en IP door FineWeb-2, in de tekst van alle documenten t/m de "
        "crawljaargrens (`kept/` en `excluded/`; `pii.py`):"
    )
    a("")
    a("| type | vervangingen (alle) | in `kept/` | vervangen door |")
    a("|---|---:|---:|---|")
    ph = {
        "iban": pii.PLACEHOLDER["iban"],
        "card": pii.PLACEHOLDER["card"],
        "phone": pii.PLACEHOLDER["phone"],
        "bsn": pii.PLACEHOLDER["bsn"],
        "address": f"{pii.PLACEHOLDER['postcode']} + huisnummer {pii.PLACEHOLDER['housenumber']}",
    }
    for k in pii.TYPES:
        a(
            f"| {k} | {fmt(s['pii']['replacements'].get(k, 0))} | {fmt(s['pii']['replacements_kept'].get(k, 0))} "
            f"| `{ph[k]}` |"
        )
    a("")
    pk, pa_ = s["pii"]["documents_with_replacement_kept"], s["pii"]["documents_with_replacement"]
    a(
        f"Documenten in `kept/` met minstens één vervanging: {fmt(pk)} ({pct(pk, d['kept'])}); in alle documenten: "
        f"{fmt(pa_)} ({pct(pa_, d['after_crawl_limit'])}). Per document staat het aantal in `pii_replaced`."
    )
    a("")
    a("## De regel")
    a("")
    a("```")
    a(m["rule"])
    a("```")
    a("")
    a(
        f"Agents: `*`, de kernlijst ({len(m['agents']['core'])}): "
        + ", ".join(f"`{x}`" for x in m["agents"]["core"])
        + f"; plus de {len(m['agents']['extended'])} andere agents van ai.robots.txt (`robots.json` @ "
        f"`{m['agents']['extended_commit'][:7]}`; volledige lijst in de JSON en in het manifest). Vergelijking van "
        "product tokens zonder hoofdlettergevoeligheid, zoals RFC 9309 voorschrijft."
    )
    a("")
    a("## Lijsten (momentopname)")
    a("")
    a("| bestand | bron | opgehaald (UTC) | sha256 |")
    a("|---|---|---|---|")
    for f in s["lists"]:
        a(f"| `{f['file']}` | {f['source']} | {f['retrieved_at']} | `{f['sha256'][:16]}…` |")
    a("")
    a("## Bekende restpunten")
    a("")
    a(
        "- **Momentopname.** De opt-outs zijn gelezen op de data hierboven. Een site die later een voorbehoud maakt, "
        "zit er nog in; een site die later een voorbehoud intrekt, blijft weg. Voor een volgende trainingsrun opnieuw "
        "meten."
    )
    a(
        "- **Hosts zonder bruikbare robots.txt blijven erin.** Een site die op het moment van meten onbereikbaar was "
        "(vaak een verdwenen domein) of op robots.txt een fout gaf, ook in de derde poging van buiten Google Cloud "
        "(bijvoorbeeld bot-bescherming die een onbekende user-agent weert), kan een voorbehoud in robots.txt hebben "
        "(gehad) dat hier niet gelezen is. Deze documenten zijn te herkennen aan `host_status` en blijven alleen weg "
        "als een andere reden geldt."
        + (
            f" {fmt(s['attempt_3']['skipped']['hosts'])} hosts met `robots_error` "
            f"({fmt(s['attempt_3']['skipped']['documents'])} documenten) zijn niet opnieuw gevraagd (derde poging "
            "gestopt, zie de regel)."
            if s["attempt_3"]["skipped"]["hosts"]
            else ""
        )
    )
    a(
        '- **Voorbehouden in natuurlijke taal** (gebruiksvoorwaarden, colofon, "alle rechten voorbehouden") zijn niet '
        "gezocht. Ook TDM-voorbehouden in HTTP-headers en HTML-meta-tags per pagina (TDMRep `tdm-reservation`, "
        "`noai`) zijn niet opgevraagd: dat zou één verzoek per document zijn."
    )
    a(
        "- **Tekst van derden.** Een opt-out geldt voor de site waar een document stond. Tekst die elders vandaan komt "
        "(geciteerd, gekopieerd, overgenomen persberichten) kan van een rechthebbende met een voorbehoud zijn en toch "
        "blijven."
    )
    a(
        "- **Persoonsgegevens in lopende tekst** (namen, adressen zonder postcode, geboortedata, gezondheid) zijn niet "
        "vervangen; de regexen vangen alleen de vormen hierboven, en kunnen ook getallen vervangen die geen "
        "persoonsgegevens zijn (bijv. een negencijferig nummer dat toevallig de elfproef haalt)."
    )
    a(
        "- **Bloklijsten zijn onvolledig.** De piraterijlijst bevat alleen domeinen die in een publieke bron staan; "
        "de Belgische lijst van de FOD Economie en niet-gepubliceerde bijlagen van Nederlandse uitspraken ontbreken. "
        "De Apertus-lijst is van januari 2025 en dekt alleen de grootste domeinen; de Opt-Out Ledger bevat alleen "
        "verzoeken die Common Crawl ontving."
    )
    a(
        "- **Kopie via een derde.** Of een voorbehoud op een website ook geldt voor een kopie uit een dataset van "
        "derden (Common Crawl, FineWeb-2), is juridisch niet beslist; dit filter behandelt het voorbehoud van nu als "
        "geldend."
    )
    a(
        "- **`excluded/` bevat de uitgesloten documenten zelf.** Alleen voor intern gebruik en analyse; niet voor "
        "training en niet voor publicatie."
    )
    a("")
    a("## Conceptzin voor een modelkaart")
    a("")
    kept_tok = t["kept"]
    left_in = d["host_status_kept"].get(rules.ERROR, 0) + d["host_status_kept"].get(rules.UNREACHABLE, 0)
    a(
        "> The web data is FineWeb-2 `nld_Latn` (revision `af9c133`), crawls up to and including CC-MAIN-2023-23, "
        f"filtered on {w['attempt_1'][0][:10]}: we removed every document whose site, at that date, disallowed its URL "
        f"in robots.txt for `*` or for any of {len(m['agents']['core']) + len(m['agents']['extended'])} AI crawlers "
        "(among them CCBot, GPTBot, Google-Extended and ClaudeBot; the ai.robots.txt list at a fixed commit plus a few "
        "more), set `Content-Signal: ai-train=no`, reserved it in `/.well-known/tdmrep.json`, or reserved text in "
        "`/ai.txt`; whose site is on the Apertus robots.txt blocklist or in Common Crawl's Opt-Out Ledger; or whose "
        "site is on a list of infringing sites from public sources (EU Counterfeit and Piracy Watch List, USTR "
        "Notorious Markets, Dutch and Belgian blocking orders). Sites whose robots.txt could not be read (unreachable, "
        "or an HTTP error, also when asked again from outside Google Cloud"
        + (
            f", which was not done for {s['attempt_3']['skipped']['hosts']:,} hosts"
            if s["attempt_3"]["skipped"]["hosts"]
            else ""
        )
        + f") count as having no known opt-out; {left_in:,} documents of such sites remain. Phone "
        "numbers, IBANs, payment card numbers, Dutch citizen service numbers and postcodes with house numbers were "
        "replaced by placeholders, in addition to FineWeb-2's own replacement of e-mail and IP addresses. This leaves "
        f"{d['kept']:,} documents ({kept_tok:,} tokens). Opt-outs stated only in page headers, meta tags or terms of "
        "use were not checked, and personal data in running text was not removed."
    )
    a("")
    return "\n".join(L) + "\n"
