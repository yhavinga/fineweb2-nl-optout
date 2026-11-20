"""The filter rule: which documents are opted out, and why.

Documents of FineWeb-2 nld_Latn whose crawl (column `dump`) is later than CRAWL_LAST are left out (reason CRAWL).
Every other document is written out with its decision: `optout` is true when any of the reasons below holds, and
`optout_reasons` lists every reason that holds, in this order (the first is the one it is counted under):

- robots_star: its path plus query is disallowed by the host's robots.txt (RFC 9309) for '*';
- robots_ai: the same for an agent in CORE_AGENTS that the robots.txt names;
- robots_ai_extended: the same for an agent of the ai.robots.txt list (EXTENDED_SOURCE) that is not in CORE_AGENTS;
- robots_signal: the robots.txt group for '*' or for a listed agent says Content-Signal ai-train=no, or has a
  Content-Usage line that withholds training for a pattern matching its path;
- tdmrep: /.well-known/tdmrep.json (W3C TDMRep) has, as the first rule whose location matches its path,
  tdm-reservation 1;
- aitxt: the host's /ai.txt (Spawning) disallows its path for '*' or a listed agent, or disallows text (a probe path
  ending in .html or .txt) for them;
- apertus: its host is in the Apertus robots.txt blocklist (swiss-ai/robots-txt-blocked-domains-multilingual);
- cc_ledger: its host or URL is in the Common Crawl Opt-Out Ledger (a domain entry covers its subdomains);
- piracy: its host is in the list of infringing sites (lists/piracy_domains.tsv).

`host_status` says what the host's robots.txt fetch gave (robots_outcome):

- ok: a 2xx; the file is parsed as RFC 9309 says (its first 500 KiB; content that is not robots.txt gives no rules);
- no_robots: 404 or 410, the host has no robots.txt: no restriction (RFC 9309 2.3.1.3);
- robots_error: a 4xx other than 404, 410 and 429 (401, 403, ...), and no 2xx, 404 or 410 on the third attempt (or
  no third attempt, see below);
- unreachable: no usable answer on two attempts at least 30 minutes apart: DNS failure, no connection, TLS failure on
  both https and http, time-out, 429 or 5xx, more than 5 redirects, a final 1xx or 3xx, a broken response;
- no_host: the URL has no host.

Attempts 1 and 2 run in Google Cloud. A host whose robots.txt gave robots_error there (many of these are a 403 from bot
protection that blocks the addresses of cloud providers) is asked for its robots.txt a third time, from a server
outside Google Cloud, with the same user agent, parser and handling of 429 (attempt3_decides):

- a 2xx makes the host ok, and the rules of that robots.txt apply (every robots reason above);
- a 404 or 410 makes it no_robots;
- any other 4xx, or no usable answer (as for unreachable), leaves it at robots_error.

`optout_checked_at` is the time of the fetch that decided: the third attempt when it gave a 2xx, 404 or 410, else the
attempt before it. tdmrep.json and ai.txt are not asked again; those of the earlier attempt apply.

The third attempt was stopped before its last units were done (15 Nov 2026): what was left were almost only
subdomains of link farms and start-page clones on one slow server, which at 2 hosts per registered domain at a time
would have taken hours more, for about 0.09 % of the documents in kept/. The hosts of the units without a result
are listed in work/fetch3-skipped.parquet (unit, host, regdom, n_le). A host on that list keeps the record of attempt
1 or 2: it stays robots_error, no reservation known. Any other robots_error host without a record of the third
attempt is an error that stops the table.

robots_error, unreachable and no_host are no opt-out: for those hosts no reservation in robots.txt is known, and the
document is opted out only when another reason holds: tdmrep.json and ai.txt (fetched unless the host is unreachable)
or the lists.
"""

from __future__ import annotations

import json
from pathlib import Path

CRAWL_LAST = "CC-MAIN-2023-23"  # "CC-MAIN-YYYY-WW" sorts as its date

REPO = "https://github.com/yhavinga/fineweb2-nl-optout"
USER_AGENT = f"fineweb2-nl-optout/1.0 (+{REPO}; one-time check of robots.txt, tdmrep.json and ai.txt)"

# The agents named in the task, those of the earlier CulturaX-nl measurement, and a few more of the same vendors.
CORE_AGENTS = [
    "GPTBot",
    "ChatGPT-User",
    "OAI-SearchBot",
    "CCBot",
    "Google-Extended",
    "ClaudeBot",
    "anthropic-ai",
    "Claude-Web",
    "Claude-SearchBot",
    "Claude-User",
    "PerplexityBot",
    "Perplexity-User",
    "Bytespider",
    "Applebot-Extended",
    "meta-externalagent",
    "meta-externalfetcher",
    "FacebookBot",
    "cohere-ai",
    "cohere-training-data-crawler",
    "Diffbot",
    "Omgilibot",
    "omgili",
    "Amazonbot",
    "ImagesiftBot",
    "Timpibot",
    "YouBot",
    "AI2Bot",
    "Ai2Bot-Dolma",
    "Google-CloudVertexBot",
    "MistralAI-User",
    "DeepSeekBot",
    "__usage_ai-training",
]
# All agents of ai.robots.txt (github.com/ai-robots-txt/ai.robots.txt, robots.json at commit 795e897, 14 Nov 2026).
EXTENDED_SOURCE = "lists/ai-robots-txt-795e897.json"
EXTENDED_COMMIT = "795e897a54ce1f926576de74f1dea4a313e3d204"

# Spawning's ai.txt generator writes one "Disallow: *.<ext>" line per file type; text is *.txt, *.pdf, *.doc, ...,
# *.html. A web page counts as reserved when either of these probe paths is disallowed.
AI_TXT_TEXT_PROBES = ["/fw2-optout-probe.html", "/fw2-optout-probe.txt"]

REASONS = [
    "robots_star",
    "robots_ai",
    "robots_ai_extended",
    "robots_signal",
    "tdmrep",
    "aitxt",
    "apertus",
    "cc_ledger",
    "piracy",
]
CRAWL = "crawl"  # the reason in decisions/ of a document after CRAWL_LAST; such documents are not written out
# the reasons that come from robots.txt: a document of a host that attempt 3 made ok, with only these reasons, would
# have been kept on attempts 1 and 2 alone
ROBOTS_REASONS = ["robots_star", "robots_ai", "robots_ai_extended", "robots_signal"]
# the robots.txt reason of each tier of agents (decide.py): '*', CORE_AGENTS, the rest of the ai.robots.txt list
TIER_REASON = {"*": "robots_star", "core": "robots_ai", "ext": "robots_ai_extended"}

# host_status: the class of the robots.txt fetch of a host, or no host
OK, ABSENT, ERROR, UNREACHABLE, NO_HOST = "ok", "no_robots", "robots_error", "unreachable", "no_host"
HOST_STATUSES = [OK, ABSENT, ERROR, UNREACHABLE, NO_HOST]


def agent_tokens(repo_dir: Path | None = None) -> tuple[set[str], set[str]]:
    """(core, extended) product tokens, lowercased as robots.txt matching compares them."""
    from robots import product_token

    base = repo_dir or Path(__file__).resolve().parent
    names = json.loads((base / EXTENDED_SOURCE).read_text())
    core = {product_token(a) for a in CORE_AGENTS}
    ext = {product_token(a) for a in names} - core
    core.discard("")
    ext.discard("")
    return core, ext


def robots_outcome(status: int | None, error: str | None) -> str:
    """The class of a robots.txt fetch: its final HTTP status, or the error when there was no response."""
    if status is None or error:
        return UNREACHABLE
    if 200 <= status < 300:
        return OK
    if status in (404, 410):
        return ABSENT
    if status == 429 or status >= 500:
        return UNREACHABLE
    if 400 <= status < 500:
        return ERROR
    return UNREACHABLE  # a 1xx or 3xx as the final status: no usable answer


def attempt3_decides(status: int | None, error: str | None) -> bool:
    """True when the third attempt on a robots_error host replaces the earlier answer: a 2xx (ok) or a 404/410
    (no_robots). Any other 4xx, or no usable answer, leaves the host at robots_error with the earlier record."""
    return robots_outcome(status, error) in (OK, ABSENT)
