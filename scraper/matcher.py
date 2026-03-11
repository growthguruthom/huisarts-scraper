"""Cross-reference signalen with praktijken using fuzzy matching."""

import re
from scraper.db import get_praktijken, get_signalen, insert_match, get_connection

# Common words that should not trigger matches on their own
STOP_WORDS = {
    "de", "het", "van", "en", "in", "op", "te", "aan", "voor", "met",
    "een", "bij", "uit", "tot", "naar", "over", "om", "als", "ook",
    "nog", "wel", "niet", "maar", "dan", "meer", "al", "zo", "door",
    "nieuwe", "nieuw", "groot", "klein", "oud", "noord", "zuid", "oost", "west",
    "huisarts", "huisartsenpraktijk", "praktijk", "gezondheidscentrum",
    "medisch", "centrum", "zorg",
}


def match_signalen(since: str = None):
    """Match all unmatched signalen against the praktijken master list."""
    praktijken = get_praktijken()
    if not praktijken:
        print("Geen praktijken in database. Run eerst --refresh-practices.")
        return

    # Build lookup indices
    postcode_index = {}
    for p in praktijken:
        pc4 = _postcode_4(p.get("postcode"))
        if pc4:
            postcode_index.setdefault(pc4, []).append(p)

    # Get unmatched signalen
    conn = get_connection()
    rows = conn.execute("""
        SELECT s.* FROM signalen s
        LEFT JOIN matches m ON s.id = m.signaal_id
        WHERE m.id IS NULL
    """).fetchall()
    conn.close()

    signalen = [dict(r) for r in rows]
    if not signalen:
        print("Geen ongematchte signalen gevonden.")
        return

    print(f"Matching {len(signalen)} signalen tegen {len(praktijken)} praktijken...")
    match_count = 0

    for signaal in signalen:
        best_match = _find_best_match(signaal, praktijken, postcode_index)
        if best_match:
            insert_match(
                praktijk_agb=best_match["agb_code"],
                signaal_id=signaal["id"],
                match_score=best_match["score"],
                match_type=best_match["type"],
            )
            match_count += 1

    print(f"Klaar: {match_count} matches gevonden van {len(signalen)} signalen.")


def _find_best_match(signaal: dict, praktijken: list, postcode_index: dict) -> dict | None:
    """Find the best matching praktijk for a signaal."""
    candidates = []

    signaal_pc4 = _postcode_4(signaal.get("postcode"))
    signaal_adres = (signaal.get("adres") or "").lower()
    signaal_titel = (signaal.get("titel") or "").lower()
    signaal_omschrijving = (signaal.get("omschrijving") or "").lower()
    signaal_text = f"{signaal_titel} {signaal_omschrijving}"

    # Method 1: Postcode + straatnaam match (strongest signal)
    if signaal_pc4 and signaal_pc4 in postcode_index:
        for p in postcode_index[signaal_pc4]:
            p_adres = (p.get("adres") or "").lower()
            if signaal_adres and p_adres:
                # Extract street name and compare
                s_street = _extract_street(signaal_adres)
                p_street = _extract_street(p_adres)
                if s_street and p_street and s_street == p_street:
                    candidates.append({
                        "agb_code": p["agb_code"],
                        "score": "hoog",
                        "type": "postcode+straat",
                        "raw_score": 2.0,
                    })

    # Method 2: Exact practice name match in signaal text
    # Only use distinctive multi-word names that aren't likely place/street names
    for p in praktijken:
        naam = (p.get("naam") or "").lower()
        core = _extract_core_name(naam)
        if not core:
            continue

        # Require at least 2 words and 8 chars to avoid matching generic
        # place names, street names, and common words
        core_words = core.split()
        if len(core_words) < 2 or len(core) < 8:
            continue

        # Skip if core name looks like a location (place name, street name)
        if _looks_like_location(core):
            continue

        # Check if the full core name appears as a whole word boundary match
        if _whole_word_match(core, signaal_text):
            candidates.append({
                "agb_code": p["agb_code"],
                "score": "hoog",
                "type": "naam",
                "raw_score": 2.0,
            })

    if not candidates:
        return None

    # Return the best candidate
    candidates.sort(key=lambda c: c["raw_score"], reverse=True)
    best = candidates[0]
    return {"agb_code": best["agb_code"], "score": best["score"], "type": best["type"]}


def _postcode_4(postcode: str | None) -> str | None:
    """Extract 4-digit postcode prefix."""
    if not postcode:
        return None
    m = re.match(r"(\d{4})", postcode.strip())
    return m.group(1) if m else None


def _extract_street(address: str) -> str | None:
    """Extract street name from an address, without house number."""
    m = re.match(r"([a-zàáâãäåèéêëìíîïòóôõöùúûüý\s.-]+)", address.strip())
    if m:
        street = m.group(1).strip().rstrip(".")
        if len(street) > 2:
            return street
    return None


def _extract_core_name(full_name: str) -> str | None:
    """Extract the distinctive part of a practice name, removing common prefixes."""
    name = re.sub(
        r"^(huisartsenpraktijk|huisartspraktijk|praktijk|gezondheidscentrum|medisch centrum|mc)\s+",
        "", full_name
    )
    # Remove very short/common words
    words = [w for w in name.split() if w not in STOP_WORDS and len(w) > 2]
    if not words:
        return None
    return " ".join(words)


def _looks_like_location(name: str) -> bool:
    """Check if a name looks like a place name, street name, or geographic term.

    These would cause false positives when matched against publication texts
    that mention the same locations.
    """
    # Common street suffixes
    street_suffixes = (
        "straat", "weg", "laan", "plein", "singel", "gracht", "kade",
        "dijk", "dreef", "hof", "pad", "ring", "steeg", "dam", "markt",
        "park", "baan", "vest", "wal",
    )
    words = name.split()

    # If the core name ends with a street suffix, it's a street name
    if any(name.endswith(s) for s in street_suffixes):
        return True

    # Common geographic patterns that appear in government documents
    geo_patterns = [
        r"^(aan|bij|op|in)\s+(de|het|den)\s+",  # "aan de IJssel", "bij de Brug"
        r"\b(centrum|dorp|wijk|buurt|kwartier)\b",  # generic area names
    ]
    for pat in geo_patterns:
        if re.search(pat, name):
            return True

    # If name is 1-2 words and could be a Dutch place name (capitalized single word)
    # Most Dutch place names are single words or "adjective + noun"
    if len(words) == 1:
        return True  # Already filtered by len < 2 requirement above

    # Filter common generic phrases that match government document titles
    generic_phrases = {
        "samen gezond", "samen sterk", "samen verder",
        "het hart", "de brug", "de poort", "de haven",
    }
    if name in generic_phrases:
        return True

    return False


def _whole_word_match(needle: str, haystack: str) -> bool:
    """Check if needle appears as a whole word/phrase in haystack."""
    pattern = r"\b" + re.escape(needle) + r"\b"
    return bool(re.search(pattern, haystack))
