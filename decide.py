"""The decision per host (from the fetched robots.txt, tdmrep.json and ai.txt) and per document (rules.py)."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from urllib.parse import urlsplit

import robots as rb
import rules
from lists import to_ascii


def split_url(url: str) -> tuple[str, str]:
    """(host, path plus query as robots.txt rules see it); host '' when the URL has none."""
    try:
        sp = urlsplit(url)
        host = sp.hostname or ""
    except ValueError:
        return "", "/"
    return to_ascii(host), rb.url_path(sp.path, sp.query)


def _effective(r: rb.Robots, core: set[str], ext: set[str]) -> tuple[list[str], dict[tuple, set[str]]]:
    """The agents checked ('*' and the listed agents the file names) and their distinct rule lists, by tier."""
    named = sorted(r.named_tokens() & (core | ext))
    lists: dict[tuple, set[str]] = {}
    for token in ["*", *named]:
        tier = "*" if token == "*" else ("core" if token in core else "ext")
        rl = r.rules_for(token)
        if any(not allow for allow, _ in rl):
            lists.setdefault(tuple(rl), set()).add(tier)
    return ["*", *named], lists


def robots_decision(body: bytes, core: set[str], ext: set[str]) -> dict:
    r = rb.parse(rb.decode_body(body))
    agents, lists = _effective(r, core, ext)
    signal = any(rb.signal_withholds_training(r.signals_for(t)) for t in agents)
    usage = sorted({p for t in agents for p in rb.usage_reservations(r.usages_for(t))})
    root_blocked = [t for t in agents if not rb.allowed(rb.compile_rules(r.rules_for(t)), "/")]
    return {
        "rules": [[sorted(tiers), [[int(a), p] for a, p in rl]] for rl, tiers in lists.items()] or None,
        "signal": signal,
        "usage": usage or None,
        "named": agents[1:] or None,
        "root_blocked": root_blocked or None,
    }


def tdmrep_decision(body: bytes) -> tuple[bool, list | None]:
    """(valid file, the rules in order as [location, reservation] when any rule reserves)."""
    try:
        data = json.loads(body.decode("utf-8", "replace").lstrip("﻿"))
    except ValueError:
        return False, None
    if not isinstance(data, list):
        return False, None
    out = []
    for item in data:
        if not isinstance(item, dict) or not isinstance(item.get("location"), str):
            continue
        v = item.get("tdm-reservation")
        if v in (0, 1, "0", "1") and not isinstance(v, bool):
            out.append([item["location"], int(v)])
    return True, (out if any(v == 1 for _, v in out) else None)


def aitxt_decision(body: bytes, core: set[str], ext: set[str]) -> tuple[bool, bool, list | None]:
    """(file has rules, text reserved for the whole host, path rule lists as in robots_decision)."""
    r = rb.parse(rb.decode_body(body))
    if not r.groups:
        return False, False, None
    agents, lists = _effective(r, core, ext)
    text = any(
        not rb.allowed(rb.compile_rules(r.rules_for(t)), probe) for t in agents for probe in rules.AI_TXT_TEXT_PROBES
    )
    return True, text, [[sorted(tiers), [[int(a), p] for a, p in rl]] for rl, tiers in lists.items()] or None


def ok_status(status) -> bool:
    return status is not None and 200 <= status < 300


def decide_host(raw: dict, core: set[str], ext: set[str]) -> dict:
    """The host's columns of the host table from its raw fetch record (fetch.py)."""
    status = rules.robots_outcome(raw.get("robots_status"), raw.get("robots_error"))
    d = {
        "host_status": status,
        "robots_rules": None,
        "signal": False,
        "usage": None,
        "named": None,
        "root_blocked": None,
        "tdm_valid": False,
        "tdm_rules": None,
        "ai_valid": False,
        "ai_text": False,
        "ai_rules": None,
    }
    if status == rules.OK:
        rd = robots_decision(raw.get("robots_body") or b"", core, ext)
        d.update(
            robots_rules=rd["rules"],
            signal=rd["signal"],
            usage=rd["usage"],
            named=rd["named"],
            root_blocked=rd["root_blocked"],
        )
    if ok_status(raw.get("tdmrep_status")):
        d["tdm_valid"], d["tdm_rules"] = tdmrep_decision(raw.get("tdmrep_body") or b"")
    if ok_status(raw.get("aitxt_status")):
        d["ai_valid"], d["ai_text"], d["ai_rules"] = aitxt_decision(raw.get("aitxt_body") or b"", core, ext)
    for k in ("robots_rules", "usage", "named", "root_blocked", "tdm_rules", "ai_rules"):
        if d[k] is not None:
            d[k] = json.dumps(d[k], ensure_ascii=False, separators=(",", ":"))
    return d


class HostEval:
    """The per-document reasons of one host (row of the host table), with its patterns compiled once."""

    def __init__(self, row: dict):
        self.host_status = row["host_status"]
        # the time of the fetch whose answer decided (the second attempt for a host retried, the third when it decided)
        self.checked_at = datetime.strptime(row["fetched_at"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        self.decided_by = row["decided_by"]
        self.signal = bool(row["signal"])
        self.robots = [
            (set(t), rb.compile_rules([(bool(a), p) for a, p in rl]))
            for t, rl in json.loads(row["robots_rules"] or "[]")
        ]
        self.usage = [rb.Pattern(p) for p in json.loads(row["usage"] or "[]")]
        self.tdm = [(rb.Pattern(loc), v) for loc, v in json.loads(row["tdm_rules"] or "[]")]
        self.ai_text = bool(row["ai_text"])
        self.ai = [rb.compile_rules([(bool(a), p) for a, p in rl]) for _, rl in json.loads(row["ai_rules"] or "[]")]
        self.apertus = bool(row["apertus"])
        self.ledger = bool(row["ledger"])
        self.ledger_paths = json.loads(row["ledger_paths"] or "[]")
        self.piracy = bool(row["piracy"])

    def reasons(self, path: str) -> list[str]:
        """The opt-out reasons (rules.REASONS, in that order) for a document of this host with this path plus query."""
        tiers: set[str] = set()
        for t, compiled in self.robots:
            if not rb.allowed(compiled, path):
                tiers |= t
        out = [rules.TIER_REASON[t] for t in ("*", "core", "ext") if t in tiers]
        if self.signal or any(p.matches(path) for p in self.usage):
            out.append("robots_signal")
        for pat, v in self.tdm:
            if pat.matches(path):  # TDMRep 6.1: the first rule in sequence that matches decides
                if v == 1:
                    out.append("tdmrep")
                break
        if self.ai_text or any(not rb.allowed(c, path) for c in self.ai):
            out.append("aitxt")
        if self.apertus:
            out.append("apertus")
        if self.ledger or (self.ledger_paths and any(path.lower().startswith(p) for p in self.ledger_paths)):
            out.append("cc_ledger")
        if self.piracy:
            out.append("piracy")
        return out
