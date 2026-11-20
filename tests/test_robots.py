import robots as rb


def allowed(text, agent, path):
    r = rb.parse(text)
    return rb.allowed(rb.compile_rules(r.rules_for(agent)), rb.normalize_path(path))


def test_rfc9309_example_groups():
    # RFC 9309 2.2.1 / 5.1 example
    text = """
User-Agent: *
Disallow: *.gif$
Disallow: /example/
Allow: /publications/

User-Agent: foobot
Disallow:/
Allow:/example/page.html
Allow:/example/allowed.gif

User-Agent: barbot
User-Agent: bazbot
Disallow: /example/page.html

User-Agent: quxbot
"""
    assert not allowed(text, "foobot", "/")
    assert allowed(text, "foobot", "/example/page.html")
    assert allowed(text, "foobot", "/example/allowed.gif")
    assert not allowed(text, "barbot", "/example/page.html")
    assert not allowed(text, "bazbot", "/example/page.html")
    assert allowed(text, "barbot", "/example/other.html")
    # quxbot has an empty group: everything allowed, it does not fall back to '*'
    assert allowed(text, "quxbot", "/example/")
    assert not allowed(text, "otherbot", "/example/x")
    assert not allowed(text, "otherbot", "/a/b.gif")
    assert allowed(text, "otherbot", "/a/b.gif?x=1")  # '$' anchors the end of path plus query
    assert allowed(text, "otherbot", "/publications/x")


def test_longest_match_and_tie():
    text = "User-agent: *\nDisallow: /a\nAllow: /a/b\nDisallow: /a/b/c\nAllow: /x\nDisallow: /x\n"
    assert not allowed(text, "*", "/a/z")
    assert allowed(text, "*", "/a/b/z")
    assert not allowed(text, "*", "/a/b/c/z")
    assert allowed(text, "*", "/x/y")  # equal length: allow wins


def test_wildcards():
    text = "User-agent: *\nDisallow: /*.php$\nDisallow: /private*/\nDisallow: /fish*\n"
    assert not allowed(text, "*", "/index.php")
    assert allowed(text, "*", "/index.php?x")
    assert not allowed(text, "*", "/private-stuff/a")
    assert not allowed(text, "*", "/fishheads")
    assert allowed(text, "*", "/Fish")  # paths are case-sensitive


def test_agent_tokens_case_and_version():
    text = "User-agent: GPTBot/1.0\nDisallow: /\n\nUser-agent: ccbot\nDisallow: /news/\n"
    assert not allowed(text, "gptbot", "/x")
    assert not allowed(text, "CCBot", "/news/a")
    assert allowed(text, "CCBot", "/blog")
    assert allowed(text, "other", "/x")
    assert rb.product_token("Mozilla/5.0 (compatible; GPTBot)") == "mozilla"
    assert rb.product_token("*") == "*"
    assert rb.product_token("ChatGPT Agent") == "chatgpt"


def test_groups_merged_and_runs():
    # two groups for the same agent are merged; a crawl-delay line does not end a run of user-agent lines
    text = "User-agent: a\nCrawl-delay: 5\nUser-agent: b\nDisallow: /x\n\nUser-agent: a\nDisallow: /y\n"
    assert not allowed(text, "a", "/x") and not allowed(text, "a", "/y")
    assert not allowed(text, "b", "/x") and allowed(text, "b", "/y")


def test_empty_disallow_and_comments_and_typos():
    assert allowed("User-agent: *\nDisallow:\n", "*", "/")
    assert not allowed("User-agent: * # all\nDisallow: / # everything\n", "*", "/a")
    assert not allowed("User-agent: *\nDissallow: /a\n", "*", "/a")
    assert not allowed("User-agent: *\nDisallow: wp-admin/\n", "*", "/wp-admin/x")  # missing leading slash


def test_rules_before_any_group_are_ignored():
    assert allowed("Disallow: /\nUser-agent: *\nAllow: /a\n", "*", "/b")


def test_percent_normalisation():
    text = "User-agent: *\nDisallow: /café\nDisallow: /%7Euser\nDisallow: /a%2Fb\n"
    assert not allowed(text, "*", "/caf%C3%A9/x")
    assert not allowed(text, "*", "/caf%c3%a9")
    assert not allowed(text, "*", "/~user/x")
    assert allowed(text, "*", "/a/b")  # %2F is reserved: not the same as '/'


def test_url_path():
    assert rb.url_path("", "") == "/"
    assert rb.url_path("/a b", "q=1") == "/a b?q=1"


def test_bom_and_size():
    body = "﻿User-agent: *\nDisallow: /\n".encode()
    r = rb.parse(rb.decode_body(body))
    assert not rb.allowed(rb.compile_rules(r.rules_for("*")), "/")
    big = b"User-agent: *\n" + b"Allow: /x\n" * 60000 + b"Disallow: /\n"
    r = rb.parse(rb.decode_body(big))  # beyond 500 KiB: not read
    assert rb.allowed(rb.compile_rules(r.rules_for("*")), "/")


def test_content_signal_and_usage():
    r = rb.parse("User-agent: *\nContent-Signal: search=yes, ai-train=no\nAllow: /\n")
    assert rb.signal_withholds_training(r.signals_for("*"))
    assert rb.signal_withholds_training(r.signals_for("gptbot"))  # falls back to '*'
    r = rb.parse("User-agent: *\nContent-Signal: search=yes, ai-train=yes\n")
    assert not rb.signal_withholds_training(r.signals_for("*"))
    r = rb.parse("User-agent: *\nContent-Usage: train-ai=n\nContent-Usage: /blog/ train-genai=n, search=y\n")
    assert rb.usage_reservations(r.usages_for("*")) == ["/", "/blog/"]
    r = rb.parse("Content-Signal: ai-train=no\nUser-agent: *\nAllow: /\n")
    assert rb.signal_withholds_training(r.signals_for("anything"))


def test_pattern_matches_like_a_regex():
    import random
    import re

    rnd = random.Random(1)
    alphabet = "/ab?*$"
    for _ in range(20000):
        pat = "/" + "".join(rnd.choice(alphabet[:5]) for _ in range(rnd.randint(0, 6)))
        if rnd.random() < 0.3:
            pat += "$"
        path = "/" + "".join(rnd.choice("/ab?") for _ in range(rnd.randint(0, 8)))
        body = pat[:-1] if pat.endswith("$") else pat
        rx = ".*".join(re.escape(x) for x in body.split("*")) + (r"\Z" if pat.endswith("$") else "")
        assert rb.Pattern(pat).matches(path) == (re.match(rx, path, re.S) is not None), (pat, path)


def test_pattern_many_wildcards_is_fast():
    import time

    pat = rb.Pattern("/" + "*a" * 30 + "*b$")
    path = "/" + "a" * 5000
    t0 = time.time()
    assert not pat.matches(path)
    assert time.time() - t0 < 0.1
