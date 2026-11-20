import pytest

import pii


def scrub(t):
    return pii.scrub(t)


@pytest.mark.parametrize(
    "text",
    [
        "Bel 06-12345678 vandaag",
        "Bel 06 12345678.",
        "Bel 0612345678",
        "tel. 06 12 34 56 78",
        "tel: 020 - 123 45 67",
        "tel: 020-1234567",
        "(020) 123 4567",
        "0123-456789",
        "+31 6 12345678",
        "+31(0)20 1234567",
        "+31 (0)20 123 45 67",
        "0031 20 1234567",
        "+31 06 12345678",
        "(+31) 6 12345678",
        "+32 2 123 45 67",
        "+32 470 12 34 56",
        "0470 12 34 56",
        "02 123 45 67",
        "09/123.45.67",
    ],
)
def test_phone_replaced(text):
    out, c = scrub(text)
    assert c["phone"] == 1, (text, out)
    assert pii.PLACEHOLDER["phone"] in out


@pytest.mark.parametrize(
    "text",
    [
        "01-01-2012 12:00",
        "op 12.05.2021 om 06.30 uur",
        "0800-1234",
        "0800-123456",
        "0900 1234567",
        "in 2012 waren er 1234567 inwoners",
        "021234567",
        "prijs 0,50 euro",
        "versie 0.12.3456",
        "ISBN 0123456789X",
        "artikel 0612345678901",
    ],
)
def test_phone_kept(text):
    out, c = scrub(text)
    assert c["phone"] == 0, (text, out)


def test_phone_longest_prefix():
    out, c = scrub("bel 06 12345678 2012")
    assert c["phone"] == 1 and out == "bel 000-0000000 2012"


@pytest.mark.parametrize(
    "text",
    [
        "NL91 ABNA 0417 1643 00",
        "NL91ABNA0417164300",
        "nl91abna0417164300",
        "BE68 5390 0754 7034",
        "DE89 3704 0044 0532 0130 00",
        "GB82 WEST 1234 5698 7654 32",
    ],
)
def test_iban_replaced(text):
    out, c = scrub("rekening " + text + " t.n.v. X")
    assert c["iban"] == 1 and out == "rekening " + pii.PLACEHOLDER["iban"] + " t.n.v. X"


@pytest.mark.parametrize("text", ["NL92 ABNA 0417 1643 00", "NL91 ABNA 0417 1643", "AB12 CDEF GHIJ KLMN"])
def test_iban_kept(text):
    assert scrub(text)[1]["iban"] == 0


@pytest.mark.parametrize(
    "text", ["4111 1111 1111 1111", "4111-1111-1111-1111", "5500000000000004", "3782 822463 10005", "6011000990139424"]
)
def test_card_replaced(text):
    out, c = scrub("kaart " + text + ".")
    assert c["card"] == 1 and out == "kaart " + pii.PLACEHOLDER["card"] + "."


@pytest.mark.parametrize(
    "text", ["4111 1111 1111 1112", "1234 5678 9012 3456", "4 5 6 7 8 9 1 2 3 4 5 6 7", "41111111111111111111111"]
)
def test_card_kept(text):
    assert scrub(text)[1]["card"] == 0


def test_bsn():
    assert pii.elfproef("111222333") and pii.elfproef("123456782")
    assert not pii.elfproef("123456789") and not pii.elfproef("000000000")
    out, c = scrub("BSN 111222333, sofinummer 1234.56.782 en ordernummer 123456789")
    assert c["bsn"] == 2 and "123456789" in out and "111222333" not in out
    assert scrub("bedrag 123456782,00")[1]["bsn"] == 0
    assert scrub("code 1234567820")[1]["bsn"] == 0


@pytest.mark.parametrize(
    "text,expected",
    [
        ("Kerkstraat 12, 1234 AB Amsterdam", "Kerkstraat 0, 0000 XX Amsterdam"),
        ("Kerkstraat 12a\n1234 AB Amsterdam", "Kerkstraat 0\n0000 XX Amsterdam"),
        ("Kerkstraat 12 1234AB Amsterdam", "Kerkstraat 0 0000 XX Amsterdam"),
        ("Burg. de Withstr. 3-5, 2345 CD Leiden", "Burg. de Withstr. 0, 0000 XX Leiden"),
        ("postcode 1234 AB 12", "postcode 0000 XX 0"),
        ("1234AB, 12a", "0000 XX, 0"),
    ],
)
def test_address_replaced(text, expected):
    out, c = scrub(text)
    assert c["address"] == 1 and out == expected


@pytest.mark.parametrize(
    "text", ["in 1234 AB Amsterdam", "postcode 1234 SA 12", "in 2012 en 2013", "0123 AB 12", "Haarlem 2011 EN"]
)
def test_address_kept(text):
    assert scrub(text)[1]["address"] == 0, scrub(text)


def test_iban_digits_not_a_phone():
    out, c = scrub("NL91 ABNA 0417 1643 00")
    assert c["iban"] == 1 and c["phone"] == 0


def test_plain_text_unchanged():
    t = "Het weer was mooi in 2019. Er kwamen 1.234 bezoekers op 3 juni 2020 om 14:00 uur."
    assert scrub(t) == (t, dict.fromkeys(pii.TYPES, 0))
