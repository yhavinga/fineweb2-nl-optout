"""robots.txt parsing and matching after RFC 9309, plus the AI-usage lines some sites put in robots.txt.

Group and token handling follow Google's open-source parser (github.com/google/robotstxt), which RFC 9309 was
written from: a user-agent line's product token is its leading run of [A-Za-z_-]; consecutive user-agent lines form
one group; only allow/disallow lines end a run of user-agent lines; all groups that name an agent are merged; an agent
named in no group gets the merged '*' groups. Matching is the longest pattern (in octets), allow winning a tie, with
'*' as a wildcard and a final '$' as an end anchor, both on the percent-normalised path plus query.

Two non-RFC lines are read as well, because they state a training preference in robots.txt:
- Content-Signal (contentsignals.org, Cloudflare): "Content-Signal: search=yes, ai-train=no" inside a group;
- Content-Usage (IETF AI Preferences drafts): "Content-Usage: [path] train-ai=n, ..." inside a group.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

MAX_BYTES = 500 * 1024  # RFC 9309 2.5: crawlers must parse at least 500 KiB

_UA_KEYS = {"user-agent", "useragent", "user agent"}
_ALLOW_KEYS = {"allow"}
# Google's parser accepts these misspellings of disallow; a site that wrote one meant it
_DISALLOW_KEYS = {"disallow", "dissallow", "dissalow", "disalow", "diasllow", "disallaw"}
_SIGNAL_KEYS = {"content-signal"}
_USAGE_KEYS = {"content-usage"}

_TOKEN = re.compile(r"[A-Za-z_-]+")
_UNRESERVED = set("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~")
_PCT = re.compile(r"%([0-9A-Fa-f]{2})")

# Content-Usage keys whose value "n" withholds use for training or text and data mining. The IETF vocabulary has
# changed between drafts ("train-ai", "train-genai", earlier "ai"/"genai", and "bots" for all automated processing);
# all of them count.
USAGE_TRAIN_KEYS = {"train-ai", "train-genai", "ai", "genai", "bots", "tdm", "ai-train"}


def product_token(value: str) -> str:
    """The product token of a user-agent value, lowercased; '*' for the wildcard; '' when there is none."""
    v = value.strip()
    if v.startswith("*") and (len(v) == 1 or v[1].isspace()):
        return "*"
    m = _TOKEN.match(v)
    return m.group(0).lower() if m else ""


def normalize_path(path: str) -> str:
    """RFC 9309 2.2.2: percent-encode octets outside US-ASCII and decode encoded unreserved characters.

    Applied to both the URL and the patterns, so that "/caf%C3%A9", "/café" and "/caf%c3%a9" compare equal and
    "/%7Euser" equals "/~user". Reserved characters stay encoded (with uppercase hex), so "%2F" is not "/".
    """
    if path.isascii() and "%" not in path:
        return path
    out = []
    for ch in path:
        if ord(ch) < 128:
            out.append(ch)
        else:
            out.extend("%%%02X" % b for b in ch.encode("utf-8", "surrogatepass"))
    s = "".join(out)

    def dec(m: re.Match) -> str:
        c = chr(int(m.group(1), 16))
        return c if c in _UNRESERVED else "%" + m.group(1).upper()

    return _PCT.sub(dec, s)


class Pattern:
    """One allow/disallow pattern, compiled once.

    '*' is matched by finding the literal pieces in order, leftmost first, which is exact for patterns whose only
    special characters are '*' and a final '$', and linear in the path (a regex of many '.*' can backtrack without
    end on a long path)."""

    __slots__ = ("raw", "length", "_parts", "_anchored")

    def __init__(self, raw: str):
        # RFC 9309 patterns start with '/'; "Disallow: wp-admin/" is read as the site meant it, "/wp-admin/"
        p = normalize_path(raw if raw.startswith(("/", "*")) else "/" + raw)
        self.raw = p
        self.length = len(p.encode("utf-8"))
        self._anchored = p.endswith("$")
        self._parts = (p[:-1] if self._anchored else p).split("*")

    def matches(self, path: str) -> bool:
        parts = self._parts
        if len(parts) == 1:
            return path == parts[0] if self._anchored else path.startswith(parts[0])
        if not path.startswith(parts[0]):
            return False
        i = len(parts[0])
        last = len(parts) - 1
        for k in range(1, last):
            j = path.find(parts[k], i)
            if j < 0:
                return False
            i = j + len(parts[k])
        if self._anchored:
            return len(path) - len(parts[last]) >= i and path.endswith(parts[last])
        return path.find(parts[last], i) >= 0


@dataclass
class Group:
    tokens: set[str] = field(default_factory=set)
    rules: list[tuple[bool, str]] = field(default_factory=list)  # (allow, pattern)
    signals: list[str] = field(default_factory=list)
    usages: list[str] = field(default_factory=list)


@dataclass
class Robots:
    groups: list[Group]
    global_signals: list[str]  # Content-Signal lines outside any group
    global_usages: list[str]

    def named_tokens(self) -> set[str]:
        return {t for g in self.groups for t in g.tokens}

    def _groups_for(self, token: str) -> list[Group]:
        token = token.lower()
        specific = [g for g in self.groups if token in g.tokens]
        if specific:
            return specific
        return [g for g in self.groups if "*" in g.tokens]

    def rules_for(self, token: str) -> list[tuple[bool, str]]:
        """The merged rules that apply to an agent with this product token (RFC 9309 2.2.1)."""
        return [r for g in self._groups_for(token) for r in g.rules]

    def signals_for(self, token: str) -> list[str]:
        return self.global_signals + [s for g in self._groups_for(token) for s in g.signals]

    def usages_for(self, token: str) -> list[str]:
        return self.global_usages + [u for g in self._groups_for(token) for u in g.usages]


def decode_body(body: bytes) -> str:
    text = body[:MAX_BYTES].decode("utf-8", "replace")
    return text[1:] if text.startswith("﻿") else text


def parse(text: str) -> Robots:
    groups: list[Group] = []
    current: Group | None = None
    in_agents = False  # the previous directive line was a user-agent line
    global_signals: list[str] = []
    global_usages: list[str] = []
    for raw in text.splitlines():
        line = raw.split("#", 1)[0].strip()
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        key = " ".join(key.strip().lower().split())
        value = value.strip()
        if key in _UA_KEYS:
            if current is None or not in_agents:
                current = Group()
                groups.append(current)
            current.tokens.add(product_token(value))
            in_agents = True
        elif key in _ALLOW_KEYS or key in _DISALLOW_KEYS:
            if current is not None and value:
                current.rules.append((key in _ALLOW_KEYS, value))
            in_agents = False
        elif key in _SIGNAL_KEYS:
            (current.signals if current is not None else global_signals).append(value)
        elif key in _USAGE_KEYS:
            (current.usages if current is not None else global_usages).append(value)
    return Robots(groups, global_signals, global_usages)


def compile_rules(rules: list[tuple[bool, str]]) -> list[tuple[bool, Pattern]]:
    return [(allow, Pattern(p)) for allow, p in rules]


def allowed(compiled: list[tuple[bool, Pattern]], path: str) -> bool:
    """RFC 9309 2.2.2: the longest matching pattern decides; allow wins a tie; no match allows."""
    best_len, best_allow = -1, True
    for allow, pat in compiled:
        if pat.length < best_len or (pat.length == best_len and not allow):
            continue
        if pat.matches(path):
            best_len, best_allow = pat.length, allow
    return best_allow


def url_path(path: str, query: str) -> str:
    """The part of a URL that robots.txt rules match: the normalised path (at least '/') plus '?query'."""
    p = path or "/"
    if not p.startswith("/"):
        p = "/" + p
    if query:
        p += "?" + query
    return normalize_path(p)


def signal_withholds_training(values: list[str]) -> bool:
    """True if a Content-Signal value says ai-train=no."""
    for v in values:
        for part in v.split(","):
            if "=" in part:
                k, val = part.split("=", 1)
                if k.strip().lower() == "ai-train" and val.strip().lower() in {"no", "n", "false", "0"}:
                    return True
    return False


def usage_reservations(values: list[str]) -> list[str]:
    """Path patterns (robots syntax; '/' for the whole site) for which a Content-Usage line withholds training."""
    out = []
    for v in values:
        v = v.strip()
        path = "/"
        if v.startswith("/") or v.startswith("*"):
            parts = v.split(None, 1)
            path, v = parts[0], (parts[1] if len(parts) > 1 else "")
        for part in v.split(","):
            if "=" in part:
                k, val = part.split("=", 1)
                if k.strip().lower() in USAGE_TRAIN_KEYS and val.strip().lower() in {"n", "no"}:
                    out.append(path)
                    break
    return out
