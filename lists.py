"""Host lists applied on top of the own opt-out check: the Apertus robots.txt blocklist, the Common Crawl Opt-Out
Ledger, and the list of infringing sites. Hosts are compared in ASCII (IDNA) form, lowercased, without a leading www."""

from __future__ import annotations

import csv
import io
import re
from collections import defaultdict
from urllib.parse import urlsplit


def to_ascii(host: str) -> str:
    host = host.strip().strip(".").lower()
    if host.isascii():
        return host
    try:
        return host.encode("idna").decode("ascii")
    except UnicodeError:
        return host


def strip_www(host: str) -> str:
    return host[4:] if host.startswith("www.") else host


def suffixes(host: str) -> list[str]:
    """The host and its parent domains, without the bare top-level label: a.b.nl -> [a.b.nl, b.nl]."""
    parts = host.split(".")
    return [".".join(parts[i:]) for i in range(max(1, len(parts) - 1))]


class Apertus:
    """swiss-ai/robots-txt-blocked-domains-multilingual, fw2_domains.txt: one host per line; exact host match."""

    def __init__(self, text: str):
        self.hosts = {strip_www(to_ascii(line)) for line in text.splitlines() if line.strip()}

    def match(self, host: str) -> bool:
        return strip_www(host) in self.hosts


class Ledger:
    """The Common Crawl Opt-Out Ledger (CSV export of the Google Sheet: requester, date, domains/URLs).

    An entry without a path (or with '/' or '/*') covers the host and its subdomains; an entry with a path covers the
    URLs on that host (and its subdomains) whose path plus query starts with it, compared case-insensitively; a
    trailing '*' is dropped and a leading '*.' in the host is the same as no prefix.
    """

    def __init__(self, text: str):
        self.domains: set[str] = set()
        self.prefixes: dict[str, list[str]] = defaultdict(list)
        self.skipped: list[str] = []
        self.entries = 0
        rows = list(csv.reader(io.StringIO(text)))
        for row in rows[1:]:
            if len(row) < 3 or not row[2].strip():
                continue
            for tok in re.split(r"[\s,;]+", row[2]):
                tok = tok.strip().strip("\"'()").replace("\\", "").rstrip(":")
                if not tok:
                    continue
                if not self._add(tok):
                    self.skipped.append(tok)

    def _add(self, tok: str) -> bool:
        try:
            sp = urlsplit(tok if "://" in tok else "http://" + tok)
            host = sp.hostname
        except ValueError:
            return False
        if not host or "." not in host:
            return False
        host = to_ascii(host)
        if host.startswith("*."):
            host = host[2:]
        host = strip_www(host)
        path = sp.path + ("?" + sp.query if sp.query else "")
        path = path.rstrip("*")
        self.entries += 1
        if path in ("", "/"):
            self.domains.add(host)
        else:
            self.prefixes[host].append(path.lower())
        return True

    def match_host(self, host: str) -> bool:
        return any(s in self.domains for s in suffixes(strip_www(host)))

    def prefixes_for(self, host: str) -> list[str]:
        return sorted({p for s in suffixes(strip_www(host)) for p in self.prefixes.get(s, [])})


class Piracy:
    """lists/piracy_domains.tsv: `entry kind category sources note`; kind 'domain' covers the domain and its
    subdomains, kind 'label' covers every registered domain whose first label is the entry (thepiratebay.*)."""

    def __init__(self, text: str):
        self.domains: set[str] = set()
        self.labels: set[str] = set()
        rows = list(csv.reader(io.StringIO(text), delimiter="\t"))
        head = rows[0]
        if head[:2] != ["entry", "kind"]:
            raise ValueError(f"piracy list: unexpected header {head}")
        for row in rows[1:]:
            if not row or not row[0].strip():
                continue
            entry, kind = row[0].strip().lower(), row[1].strip()
            if kind == "domain":
                self.domains.add(strip_www(to_ascii(entry)))
            elif kind == "label":
                self.labels.add(entry)
            else:
                raise ValueError(f"piracy list: kind {kind!r} for {entry!r}")

    def match(self, host: str, regdom: str) -> bool:
        if any(s in self.domains for s in suffixes(strip_www(host))):
            return True
        return bool(regdom) and regdom.split(".", 1)[0] in self.labels
