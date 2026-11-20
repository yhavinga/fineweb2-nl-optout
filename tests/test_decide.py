import json

import decide
import rules
from lists import Apertus, Ledger, Piracy

CORE, EXT = rules.agent_tokens()


def row(raw, **flags):
    d = decide.decide_host(raw, CORE, EXT)
    base = {
        "apertus": False,
        "ledger": False,
        "ledger_paths": None,
        "piracy": False,
        "fetched_at": "2026-11-14T16:00:00Z",
        "decided_by": 1,
    }
    base.update(flags)
    return {**d, **base}


def raw(status=200, body=b"", error=None, tdm=None, ai=None):
    r = {"robots_status": status, "robots_error": error, "robots_body": body}
    if tdm is not None:
        r.update(tdmrep_status=200, tdmrep_body=tdm)
    if ai is not None:
        r.update(aitxt_status=200, aitxt_body=ai)
    return r


def reasons(r, url):
    _, path = decide.split_url(url)
    return decide.HostEval(r).reasons(path)


def test_agent_lists():
    assert {
        "gptbot",
        "ccbot",
        "google-extended",
        "claudebot",
        "anthropic-ai",
        "perplexitybot",
        "bytespider",
        "applebot-extended",
        "meta-externalagent",
        "cohere-ai",
        "diffbot",
        "omgilibot",
        "amazonbot",
        "chatgpt-user",
    } <= CORE
    assert "petalbot" in EXT and "petalbot" not in CORE
    assert "" not in CORE | EXT and "*" not in CORE | EXT


def test_outcomes():
    assert rules.robots_outcome(200, None) == rules.OK
    assert rules.robots_outcome(404, None) == rules.ABSENT
    assert rules.robots_outcome(410, None) == rules.ABSENT
    assert rules.robots_outcome(403, None) == rules.ERROR
    assert rules.robots_outcome(401, None) == rules.ERROR
    assert rules.robots_outcome(429, None) == rules.UNREACHABLE
    assert rules.robots_outcome(503, None) == rules.UNREACHABLE
    assert rules.robots_outcome(None, "dns") == rules.UNREACHABLE
    assert rules.robots_outcome(None, "redirects") == rules.UNREACHABLE


def test_attempt3_decides():
    # a 2xx or a 404/410 on attempt 3 replaces the robots_error of before; any other answer leaves it
    assert rules.attempt3_decides(200, None) and rules.attempt3_decides(204, None)
    assert rules.attempt3_decides(404, None) and rules.attempt3_decides(410, None)
    for st, err in [(403, None), (401, None), (451, None), (429, None), (503, None), (301, None), (None, "dns")]:
        assert not rules.attempt3_decides(st, err), (st, err)
    assert set(rules.ROBOTS_REASONS) <= set(rules.REASONS)
    assert [r for r in rules.REASONS if r.startswith("robots_")] == rules.ROBOTS_REASONS


def test_robots_star_and_agents_and_tiers():
    body = (
        b"User-agent: *\nDisallow: /admin/\n\nUser-agent: GPTBot\nDisallow: /\n\nUser-agent: PetalBot\nDisallow: /p/\n"
    )
    r = row(raw(body=body))
    assert reasons(r, "https://x.nl/a") == ["robots_ai"]
    assert reasons(r, "https://x.nl/admin/x") == ["robots_star", "robots_ai"]
    assert reasons(r, "https://x.nl/p/x") == ["robots_ai", "robots_ai_extended"]
    assert json.loads(r["root_blocked"]) == ["gptbot"]
    r = row(raw(body=b"User-agent: PetalBot\nDisallow: /\n"))
    assert reasons(r, "https://x.nl/a") == ["robots_ai_extended"]  # the wider list only


def test_robots_unlisted_agent_ignored():
    r = row(raw(body=b"User-agent: Googlebot\nDisallow: /\n"))
    assert reasons(r, "https://x.nl/a") == []


def test_no_usable_robots_is_no_optout():
    # no robots.txt, an error on robots.txt and an unreachable host: no reservation known, the document stays
    for st, err, status in [
        (404, None, rules.ABSENT),
        (410, None, rules.ABSENT),
        (403, None, rules.ERROR),
        (401, None, rules.ERROR),
        (429, None, rules.UNREACHABLE),
        (500, None, rules.UNREACHABLE),
        (None, "timeout", rules.UNREACHABLE),
        (None, "dns", rules.UNREACHABLE),
    ]:
        r = row(raw(status=st, error=err))
        assert r["host_status"] == status
        assert reasons(r, "https://x.nl/a") == [], (st, err)
    # the other reasons still hold for such a host
    tdm = b'[{"location": "/", "tdm-reservation": 1}]'
    assert reasons(row(raw(status=403, tdm=tdm)), "https://x.nl/a") == ["tdmrep"]
    assert reasons(row(raw(status=None, error="dns"), apertus=True, piracy=True), "https://x.nl/a") == [
        "apertus",
        "piracy",
    ]


def test_checked_at():
    from datetime import datetime, timezone

    ev = decide.HostEval(row(raw(status=404), fetched_at="2026-11-14T16:45:03Z"))
    assert ev.checked_at == datetime(2026, 11, 14, 16, 45, 3, tzinfo=timezone.utc)


def test_signal_and_usage():
    r = row(raw(body=b"User-agent: *\nContent-Signal: search=yes, ai-train=no\nAllow: /\n"))
    assert reasons(r, "https://x.nl/a") == ["robots_signal"]
    r = row(raw(body=b"User-agent: *\nContent-Usage: /blog/ train-ai=n\nAllow: /\n"))
    assert reasons(r, "https://x.nl/blog/a") == ["robots_signal"]
    assert reasons(r, "https://x.nl/a") == []


def test_tdmrep_first_match_decides():
    tdm = json.dumps([{"location": "/free/*", "tdm-reservation": 0}, {"location": "/", "tdm-reservation": 1}]).encode()
    r = row(raw(status=404, tdm=tdm))
    assert reasons(r, "https://x.nl/news/a") == ["tdmrep"]
    assert reasons(r, "https://x.nl/free/a") == []
    r = row(raw(status=404, tdm=b'[{"location": "/", "tdm-reservation": 0}]'))
    assert r["tdm_valid"] and r["tdm_rules"] is None
    r = row(raw(status=404, tdm=b"<html>not found</html>"))
    assert not r["tdm_valid"]
    r = row(raw(status=404, tdm=b'[{"location": "/", "tdm-reservation": true}]'))
    assert r["tdm_rules"] is None


def test_aitxt():
    spawning_text = b"# Spawning AI\nUser-Agent: *\nDisallow: *.txt\nDisallow: *.html\nDisallow: *.jpg\n"
    r = row(raw(status=404, ai=spawning_text))
    assert r["ai_text"] and reasons(r, "https://x.nl/a") == ["aitxt"]
    images_only = b"User-Agent: *\nDisallow: *.jpg\nAllow: *.html\nAllow: *.txt\n"
    r = row(raw(status=404, ai=images_only))
    assert not r["ai_text"] and reasons(r, "https://x.nl/a") == []
    assert reasons(r, "https://x.nl/foto.jpg") == ["aitxt"]
    r = row(raw(status=404, ai=b"<!doctype html><html><body>Page not found</body></html>"))
    assert not r["ai_valid"] and not r["ai_text"]


def test_lists():
    a = Apertus("www.nu.nl\nexample.com\n")
    assert a.match("nu.nl") and a.match("www.nu.nl") and not a.match("sport.nu.nl")
    led = Ledger(
        "Publisher/Requester,Date of notice,List of domains/URLs\nLAST UPDATED: 2026 Nov 13,,\n,,\n"
        'DPG,2026-09-23,"ad.nl, www.volkskrant.nl"\nX,2025-01-01,https://www.caliper.com/Maptitude/blog/*\n'
        "Y,2025-01-01,*.collected.reviews\nZ,2025-01-01,(2024-03-16)\n"
    )
    assert led.match_host("www.ad.nl") and led.match_host("sport.ad.nl") and led.match_host("volkskrant.nl")
    assert not led.match_host("bad.nl") and led.match_host("a.collected.reviews")
    assert led.prefixes_for("caliper.com") == ["/maptitude/blog/"]
    assert "(2024-03-16)" in led.skipped or "2024-03-16" in led.skipped
    p = Piracy(
        "entry\tkind\tcategory\tsources\tnote\nthepiratebay\tlabel\ttorrent\tS1\t\nlibgen.rs\tdomain\tpiracy-books\tS2\t\n"
    )
    assert p.match("thepiratebay.org", "thepiratebay.org") and p.match("x.libgen.rs", "libgen.rs")
    assert not p.match("libgen.nl", "libgen.nl")


def test_ledger_paths_in_eval():
    r = row(raw(status=404), ledger_paths=json.dumps(["/maptitude/blog/"]))
    assert reasons(r, "https://www.caliper.com/Maptitude/blog/x") == ["cc_ledger"]
    assert reasons(r, "https://www.caliper.com/other") == []


def test_order_of_reasons():
    body = b"User-agent: *\nDisallow: /\nContent-Signal: ai-train=no\n"
    r = row(raw(body=body), apertus=True, piracy=True)
    got = reasons(r, "https://x.nl/a")
    assert got == ["robots_star", "robots_signal", "apertus", "piracy"]
    assert [x for x in rules.REASONS if x in got] == got


def test_split_url():
    assert decide.split_url("https://WWW.Example.NL:8080/a b?x=1#f") == ("www.example.nl", "/a b?x=1")
    assert decide.split_url("https://bücher.de/x")[0] == "xn--bcher-kva.de"
    assert decide.split_url("not a url")[0] == ""
    assert decide.split_url("http://[::1")[0] == ""


def test_piracy_list_of_the_repo():
    from pathlib import Path

    base = Path(__file__).resolve().parent.parent / "lists"
    p = Piracy((base / "piracy_domains.tsv").read_text())
    assert len(p.domains) > 500 and "thepiratebay" in p.labels
    assert p.match("thepiratebay.org", "thepiratebay.org") and p.match("libgen.xyz", "libgen.xyz")
    assert not p.match("www.nu.nl", "nu.nl") and not p.match("nl.wikipedia.org", "wikipedia.org")
    ids = {line.split("\t")[0] for line in (base / "piracy_sources.tsv").read_text().splitlines()[1:] if line}
    for line in (base / "piracy_domains.tsv").read_text().splitlines()[1:]:
        assert set(line.split("\t")[3].split(",")) <= ids, line
