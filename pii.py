"""Replacement of personal data that FineWeb-2 leaves in the text.

FineWeb-2 already anonymises e-mail addresses and IP addresses. This module replaces, in this order:

- iban: IBANs of the SEPA and IBAN registry countries, contiguous or in groups of four, with a valid length for the
  country and a valid mod-97 check;
- card: payment card numbers, contiguous or grouped 4-4-4-x / 4-6-x, of a known card scheme prefix and length,
  passing the Luhn check;
- phone: Dutch numbers (10 digits nationally, or +31/0031 with 9 digits) and Belgian numbers (+32/0032 with 8 or 9
  digits; nationally 10 digits for mobiles, 9 digits for landlines when written in at least three groups); not the
  Dutch service numbers 0800/0900/0906/0909;
- bsn: Dutch citizen service numbers: 9 digits, or 4.2.3 with dots, passing the elfproef;
- address: a Dutch postcode (1234 AB) next to a house number, as "<street> 12, 1234 AB" (comma, spaces or a line
  break between them) or "1234 AB 12"; the postcode and the house number are replaced, the street is kept.

Each match is replaced by a fixed placeholder that is invalid by construction (an IBAN with check digits 00, a
postcode starting with 0, a phone number starting with 000), as FineWeb replaces e-mail addresses with fixed ones.
"""

from __future__ import annotations

import re

PLACEHOLDER = {
    "iban": "NL00 BANK 0000 0000 00",
    "card": "0000 0000 0000 0000",
    "phone": "000-0000000",
    "bsn": "000000000",
    "postcode": "0000 XX",
    "housenumber": "0",
}
TYPES = ["iban", "card", "phone", "bsn", "address"]

# ---------------------------------------------------------------------------------------------------------- IBAN
IBAN_LENGTH = {
    "AD": 24,
    "AE": 23,
    "AL": 28,
    "AT": 20,
    "AZ": 28,
    "BA": 20,
    "BE": 16,
    "BG": 22,
    "BH": 22,
    "BR": 29,
    "BY": 28,
    "CH": 21,
    "CR": 22,
    "CY": 28,
    "CZ": 24,
    "DE": 22,
    "DK": 18,
    "DO": 28,
    "EE": 20,
    "EG": 29,
    "ES": 24,
    "FI": 18,
    "FO": 18,
    "FR": 27,
    "GB": 22,
    "GE": 22,
    "GI": 23,
    "GL": 18,
    "GR": 27,
    "GT": 28,
    "HR": 21,
    "HU": 28,
    "IE": 22,
    "IL": 23,
    "IQ": 23,
    "IS": 26,
    "IT": 27,
    "JO": 30,
    "KW": 30,
    "KZ": 20,
    "LB": 28,
    "LC": 32,
    "LI": 21,
    "LT": 20,
    "LU": 20,
    "LV": 21,
    "MC": 27,
    "MD": 24,
    "ME": 22,
    "MK": 19,
    "MR": 27,
    "MT": 31,
    "MU": 30,
    "NL": 18,
    "NO": 15,
    "PK": 24,
    "PL": 28,
    "PS": 29,
    "PT": 25,
    "QA": 29,
    "RO": 24,
    "RS": 22,
    "SA": 24,
    "SC": 31,
    "SE": 24,
    "SI": 19,
    "SK": 24,
    "SM": 27,
    "ST": 25,
    "SV": 28,
    "TL": 23,
    "TN": 24,
    "TR": 26,
    "UA": 29,
    "VA": 22,
    "VG": 24,
    "XK": 20,
}
_IBAN = re.compile(
    r"(?<![A-Za-z0-9])[A-Za-z]{2}\d{2}(?:[ ]?[A-Za-z0-9]{4}){2,7}(?:[ ]?[A-Za-z0-9]{1,3})?(?![A-Za-z0-9])"
)


def iban_valid(s: str) -> bool:
    c = s.replace(" ", "").upper()
    if IBAN_LENGTH.get(c[:2]) != len(c):
        return False
    r = c[4:] + c[:4]
    return int("".join(str(int(ch, 36)) for ch in r)) % 97 == 1


def _iban_sub(m: re.Match) -> tuple[str, int]:
    s = m.group(0)
    # the last group may be the start of the next word ("... 7034 t.n.v."): the longest valid prefix at a space
    for cut in [len(s)] + [i for i in range(len(s) - 1, 0, -1) if s[i] == " "]:
        if iban_valid(s[:cut]):
            return PLACEHOLDER["iban"] + s[cut:], 1
    return s, 0


# ---------------------------------------------------------------------------------------------------------- cards
_CARD = re.compile(
    r"(?<![\w.,/-])(?:\d{13,19}|\d{4}([ -])\d{4}\1\d{4}\1\d{1,7}|\d{4}([ -])\d{6}\2\d{4,5})(?![\w]|[.,/-]\d)"
)


def luhn(d: str) -> bool:
    total = 0
    for i, ch in enumerate(reversed(d)):
        n = int(ch)
        if i % 2:
            n *= 2
            if n > 9:
                n -= 9
        total += n
    return total % 10 == 0


def card_scheme_ok(d: str) -> bool:
    n = len(d)
    p2, p3, p4 = int(d[:2]), int(d[:3]), int(d[:4])
    if d[0] == "4":
        return n in (13, 16, 19)  # Visa
    if 51 <= p2 <= 55 or 2221 <= p4 <= 2720:
        return n == 16  # Mastercard
    if p2 in (34, 37):
        return n == 15  # American Express
    if p2 in (36, 38, 39) or 300 <= p3 <= 305:
        return 14 <= n <= 19  # Diners
    if p4 == 6011 or p2 == 65 or 644 <= p3 <= 649:
        return 16 <= n <= 19  # Discover
    if p2 == 62:
        return 16 <= n <= 19  # UnionPay
    if 3528 <= p4 <= 3589:
        return 16 <= n <= 19  # JCB
    if p2 == 50 or 56 <= p2 <= 69:
        return 13 <= n <= 19  # Maestro
    return False


def card_valid(s: str) -> bool:
    d = re.sub(r"[ -]", "", s)
    return 13 <= len(d) <= 19 and card_scheme_ok(d) and luhn(d)


# ---------------------------------------------------------------------------------------------------------- phone
_GROUPS = r"\d{2,4}(?:[ .]?\d{2,4}){0,3}"
_PHONE_BODY = (
    r"(?:(?P<intl>\(?(?:\+|00)[ ]?(?P<cc>31|32)\)?[ ]?(?:\(0\)[ ]?)?\d{1,3}(?:[ ]?[-/][ ]?|[ .])?" + _GROUPS + r")"
    r"|(?P<nat>(?:\(0\d{1,3}\)|0\d{1,3})(?:[ ]?[-/][ ]?|[ .])?" + _GROUPS + r"))"
)
_PHONE = re.compile(r"(?<![\w+(/.,-])" + _PHONE_BODY + r"(?![\w]|[.,/-]\d)")
_PHONE_FULL = re.compile(_PHONE_BODY)
_SERVICE = ("0800", "0900", "0906", "0909")


def phone_valid(m: re.Match) -> bool:
    s = m.group(0)
    if m.group("intl"):
        rest = re.sub(r"\D", "", s.split(m.group("cc"), 1)[1].replace("(0)", ""))
        if rest.startswith("0"):  # "+31 06 ...": the national 0 written after the country code
            rest = rest[1:]
        return len(rest) == 9 if m.group("cc") == "31" else len(rest) in (8, 9)
    digits = re.sub(r"\D", "", s)
    if digits[1] == "0" or digits.startswith(_SERVICE):
        return False
    if len(digits) == 10:
        return True
    # Belgian landline, 9 digits: only when written in groups (02 123 45 67, 09/123.45.67), as plain 9-digit
    # strings starting with 0 are too often something else
    return len(digits) == 9 and len(re.findall(r"\d+", s)) >= 3


def _phone_ok(s: str) -> bool:
    m = _PHONE_FULL.fullmatch(s)
    return m is not None and phone_valid(m)


def _phone_sub(m: re.Match) -> tuple[str, int]:
    s = m.group(0)
    if phone_valid(m):
        return PLACEHOLDER["phone"], 1
    # more digit groups than a phone number ("06 12345678 2012"): the longest prefix ending at a group boundary
    for cut in reversed([i for i in range(1, len(s)) if s[i] in " ." and s[i - 1].isdigit()]):
        if _phone_ok(s[:cut]):
            return PLACEHOLDER["phone"] + s[cut:], 1
    return s, 0


# ---------------------------------------------------------------------------------------------------------- BSN
_BSN = re.compile(r"(?<![\w.,/-])(?:\d{4}\.\d{2}\.\d{3}|\d{9})(?![\w]|[.,/-]\d)")


def elfproef(s: str) -> bool:
    d = [int(c) for c in s if c.isdigit()]
    if len(d) != 9 or not any(d):
        return False
    return (sum(d[i] * (9 - i) for i in range(8)) - d[8]) % 11 == 0


# ---------------------------------------------------------------------------------------------------------- address
_POSTCODE = re.compile(r"(?<![\w-])[1-9]\d{3}[ ]?(?!SA|SD|SS)[A-Z]{2}(?![\w])")
_NUM = r"\d{1,5}(?:[ ]?-?[ ]?[A-Za-z]{1,2}(?![A-Za-z])|[-/]\d{1,4})?"
# "<street> 12a, " right before a postcode: the street ends in a letter or a dot (Str.), then the number, then a
# comma, a line break or spaces
_BEFORE = re.compile(r"[A-Za-zÀ-ÖØ-öø-ÿ.][ \t]+(?P<num>" + _NUM + r")(?:[ \t]*,[ \t]*|[ \t]*\n[ \t]*|[ \t]+)\Z")
# "1234 AB 12" / "1234AB, 12a": the number right after the postcode
_AFTER = re.compile(r"(?:[ \t]*,[ \t]*|[ \t]+)(?P<num>\d{1,5}(?:[ ]?[A-Za-z](?![A-Za-z])|-\d{1,4})?)(?![\w]|[.,]\d)")


def _address_spans(text: str) -> list[tuple[int, int, str]]:
    spans = []
    for m in _POSTCODE.finditer(text):
        a, b = m.span()
        before = _BEFORE.search(text, max(0, a - 40), a)
        if before:
            spans.append((before.start("num"), before.end("num"), PLACEHOLDER["housenumber"]))
            spans.append((a, b, PLACEHOLDER["postcode"]))
            continue
        after = _AFTER.match(text, b)
        if after:
            spans.append((a, b, PLACEHOLDER["postcode"]))
            spans.append((after.start("num"), after.end("num"), PLACEHOLDER["housenumber"]))
    return spans


def _replace(text: str, rx: re.Pattern, ok, placeholder: str) -> tuple[str, int]:
    n = 0

    def sub(m: re.Match) -> str:
        nonlocal n
        if ok(m):
            n += 1
            return placeholder
        return m.group(0)

    return rx.sub(sub, text), n


def _sub_counted(text: str, rx: re.Pattern, fn) -> tuple[str, int]:
    n = 0

    def sub(m: re.Match) -> str:
        nonlocal n
        out, k = fn(m)
        n += k
        return out

    return rx.sub(sub, text), n


def scrub(text: str) -> tuple[str, dict[str, int]]:
    """The text with the personal data above replaced, and the number of replacements per type."""
    counts = dict.fromkeys(TYPES, 0)
    text, counts["iban"] = _sub_counted(text, _IBAN, _iban_sub)
    text, counts["card"] = _replace(text, _CARD, lambda m: card_valid(m.group(0)), PLACEHOLDER["card"])
    text, counts["phone"] = _sub_counted(text, _PHONE, _phone_sub)
    text, counts["bsn"] = _replace(text, _BSN, lambda m: elfproef(m.group(0)), PLACEHOLDER["bsn"])
    spans = _address_spans(text)
    if spans:
        out, last = [], 0
        for a, b, rep in sorted(spans):
            if a < last:
                continue
            out.append(text[last:a])
            out.append(rep)
            last = b
        out.append(text[last:])
        text = "".join(out)
        counts["address"] = sum(1 for _, _, rep in spans if rep == PLACEHOLDER["postcode"])
    return text, counts
