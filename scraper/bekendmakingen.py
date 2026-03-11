"""Query omgevingsvergunningen from officielebekendmakingen.nl via SRU API."""

import re
import time
import requests
from xml.etree import ElementTree as ET
from bs4 import BeautifulSoup
from tqdm import tqdm
from scraper.db import insert_signaal

SRU_BASE = "https://repository.overheid.nl/sru"

# CQL queries: use separate AND terms (not quoted phrases)
SEARCH_QUERIES = [
    ("huisartsenpraktijk", "omgevingsvergunning"),
    ("huisartsenpraktijk", "verbouwing"),
    ("huisartsenpraktijk", "nieuwbouw"),
    ("huisarts", "verbouwing"),
    ("huisarts", "nieuwbouw"),
    ("medisch centrum", "omgevingsvergunning"),
    ("gezondheidscentrum", "verbouwing"),
    ("gezondheidscentrum", "omgevingsvergunning"),
]

HEADERS = {
    "User-Agent": "HuisartsScraper/1.0 (lead-generation tool)",
    "Accept": "application/xml",
}

SRU_NS = {"sru": "http://docs.oasis-open.org/ns/search-ws/sruResponse"}
GZD_NS = {"gzd": "http://standaarden.overheid.nl/sru"}
DC_NS = {
    "dcterms": "http://purl.org/dc/terms/",
    "overheid": "http://standaarden.overheid.nl/owms/terms/",
}


def scrape_bekendmakingen(since: str, delay: float = 1.0) -> int:
    """Query SRU API for building permits related to GP practices.

    Args:
        since: Date string in YYYY-MM-DD format
        delay: Seconds between API requests
    """
    print(f"Bekendmakingen ophalen sinds {since}...")
    session = requests.Session()
    session.headers.update(HEADERS)
    total_new = 0
    seen_urls = set()

    for terms in SEARCH_QUERIES:
        # Build CQL: each term as separate AND clause
        term_clauses = " AND ".join(f"cql.textAndIndexes={t}" for t in terms)
        cql = f"c.product-area==officielepublicaties AND {term_clauses} AND dt.modified>={since}"
        label = " + ".join(terms)
        print(f"  Query: {label}")
        new = _fetch_all_pages(session, cql, seen_urls, delay)
        total_new += new
        print(f"    → {new} nieuwe signalen")

    print(f"Klaar: {total_new} nieuwe signalen opgeslagen.")
    return total_new


def _fetch_all_pages(session: requests.Session, cql: str, seen_urls: set, delay: float) -> int:
    """Fetch all pages of SRU results for a query."""
    start = 1
    max_records = 100
    new_count = 0

    while True:
        params = {
            "operation": "searchRetrieve",
            "version": "2.0",
            "startRecord": start,
            "maximumRecords": max_records,
            "query": cql,
        }

        try:
            resp = session.get(SRU_BASE, params=params, timeout=30)
            resp.raise_for_status()
        except requests.RequestException as e:
            print(f"    API fout: {e}")
            break

        root = ET.fromstring(resp.content)
        total_str = root.findtext(".//sru:numberOfRecords", namespaces=SRU_NS)
        total = int(total_str) if total_str else 0

        if start == 1 and total > 0:
            print(f"    Totaal resultaten: {total}")

        records = root.findall(".//sru:record", SRU_NS)
        if not records:
            break

        for record in records:
            data = record.find("sru:recordData", SRU_NS)
            if data is None:
                continue

            parsed = _parse_record(data)
            if not parsed or not parsed.get("bron_url"):
                continue

            if parsed["bron_url"] in seen_urls:
                continue
            seen_urls.add(parsed["bron_url"])

            result = insert_signaal(
                type="omgevingsvergunning",
                titel=parsed.get("titel"),
                omschrijving=parsed.get("omschrijving"),
                adres=parsed.get("adres"),
                postcode=parsed.get("postcode"),
                stad=parsed.get("stad"),
                gemeente=parsed.get("gemeente"),
                bron_url=parsed.get("bron_url"),
                publicatiedatum=parsed.get("publicatiedatum"),
            )
            if result is not None:
                new_count += 1

        # Check if there are more pages
        next_pos = root.findtext(".//sru:nextRecordPosition", namespaces=SRU_NS)
        if next_pos and int(next_pos) <= total:
            start = int(next_pos)
            time.sleep(delay)
        else:
            break

    return new_count


def _parse_record(data_elem) -> dict | None:
    """Parse a single SRU record into a signaal dict."""
    result = {}

    # Combine all namespaces for searching
    all_ns = {**DC_NS, **GZD_NS, **SRU_NS}

    # Title
    title = _find_text(data_elem, [
        ".//dcterms:title",
    ], all_ns)
    result["titel"] = title

    # Identifier (URL)
    identifier = _find_text(data_elem, [
        ".//dcterms:identifier",
    ], all_ns)

    if identifier:
        if identifier.startswith("http"):
            result["bron_url"] = identifier
        else:
            result["bron_url"] = f"https://zoek.officielebekendmakingen.nl/{identifier}.html"
    else:
        return None

    # Date (modified or available)
    date = _find_text(data_elem, [
        ".//dcterms:modified",
        ".//dcterms:available",
    ], all_ns)
    if date:
        m = re.match(r"(\d{4}-\d{2}-\d{2})", date)
        result["publicatiedatum"] = m.group(1) if m else date

    # Creator (often the municipality)
    creator = _find_text(data_elem, [
        ".//dcterms:creator",
    ], all_ns)
    result["gemeente"] = creator

    # Spatial (location info)
    spatial = _find_text(data_elem, [
        ".//dcterms:spatial",
    ], all_ns)
    if spatial:
        result["stad"] = spatial

    return result


def _find_text(elem, xpaths: list[str], namespaces: dict) -> str | None:
    """Try multiple XPaths and return the first match."""
    for xpath in xpaths:
        found = elem.find(xpath, namespaces)
        if found is not None and found.text:
            return found.text.strip()
    return None


def enrich_signaal_details(delay: float = 1.0):
    """Extract address info from signalen titles and detail pages.

    Phase 1: Extract addresses from titles (no HTTP needed, instant).
    Phase 2: Fetch detail pages for remaining signalen without addresses.
    """
    import sqlite3 as _sqlite3
    from scraper.db import DB_PATH, get_connection

    # Read signalen using readonly connection
    conn = get_connection(readonly=True)
    rows = conn.execute("""
        SELECT id, titel, bron_url FROM signalen
        WHERE type = 'omgevingsvergunning'
        AND adres IS NULL
        AND bron_url IS NOT NULL
    """).fetchall()
    conn.close()

    if not rows:
        print("Geen signalen om te verrijken.")
        return

    signalen = [dict(r) for r in rows]
    print(f"Verrijken van {len(signalen)} signalen met adresgegevens...")

    # Use a single write connection for all updates (avoids PRAGMA lock per call)
    write_conn = _sqlite3.connect(str(DB_PATH), timeout=30)

    # Phase 1: Extract from titles (fast, no HTTP)
    title_enriched = 0
    remaining = []
    for s in signalen:
        titel = s.get("titel") or ""
        result = _extract_address_from_text(titel)
        if result["adres"] or result["postcode"]:
            write_conn.execute(
                "UPDATE signalen SET adres = ?, postcode = ? WHERE id = ?",
                (result["adres"], result["postcode"], s["id"])
            )
            title_enriched += 1
        else:
            remaining.append(s)

    write_conn.commit()
    print(f"  Fase 1 (titels): {title_enriched} adressen gevonden")

    if not remaining:
        write_conn.close()
        print(f"Verrijkt: {title_enriched} signalen met adresgegevens.")
        return

    # Phase 2: Fetch detail pages for remaining
    print(f"  Fase 2: {len(remaining)} detail-pagina's ophalen...")
    session = requests.Session()
    session.headers.update({
        "User-Agent": HEADERS["User-Agent"],
        "Accept": "text/html",
    })

    page_enriched = 0
    for i, s in enumerate(remaining):
        try:
            resp = session.get(s["bron_url"], timeout=15)
            if resp.status_code != 200:
                continue

            soup = BeautifulSoup(resp.text, "lxml")
            main = soup.find("main") or soup.find(id="broodtekst") or soup.find(class_="bekendmaking-tekst")
            text = main.get_text(" ", strip=True) if main else ""
            if len(text) < 50:
                text = soup.get_text(" ", strip=True)

            result = _extract_address_from_text(text)
            if result["adres"] or result["postcode"]:
                write_conn.execute(
                    "UPDATE signalen SET adres = ?, postcode = ? WHERE id = ?",
                    (result["adres"], result["postcode"], s["id"])
                )
                page_enriched += 1

            # Commit every 10 updates
            if (i + 1) % 10 == 0:
                write_conn.commit()
                print(f"    {i+1}/{len(remaining)} pagina's verwerkt ({page_enriched} adressen)")

            time.sleep(delay)
        except Exception as e:
            print(f"  Fout bij {s['bron_url']}: {e}")
            continue

    write_conn.commit()
    write_conn.close()

    total = title_enriched + page_enriched
    print(f"Verrijkt: {total} signalen ({title_enriched} uit titels, {page_enriched} uit pagina's).")


def _extract_address_from_text(text: str) -> dict:
    """Extract address and postcode from text.

    Returns dict with 'adres' and 'postcode' keys (either can be None).
    """
    result = {"adres": None, "postcode": None}

    # Extract postcode (4 digits + 2 letters)
    m = re.search(r"\b(\d{4}\s?[A-Z]{2})\b", text)
    if m:
        result["postcode"] = m.group(1).replace(" ", "")

    # Extract street address with house number
    # Pattern: StreetName + number, e.g. "Stationsstraat 3" or "Goudenregenstraat 30"
    addr_patterns = [
        # Street with number (most specific)
        r"([A-Z][a-zàáâãäåèéêëìíîïòóôõöùúûüý]+(?:straat|weg|laan|plein|singel|gracht|kade|dijk|dreef|hof|pad|ring|steeg|dam|markt|park|baan|vest|wal)\s+\d+[a-zA-Z]?(?:\s*(?:en|tot en met|t/m|-)\s*\d+[a-zA-Z]?)?)",
        # "gelegen aan/op/bij" + location (but avoid navigation text)
        r"(?:gelegen\s+(?:aan|op|bij|te)|ter hoogte van|ter plaatse van)\s+(?:de\s+)?([A-Z][a-zàáâãäåèéêëìíîïòóôõöùúûüý]+(?:\s+[a-zàáâãäåèéêëìíîïòóôõöùúûüý]+)*\s+\d+[a-zA-Z]?)",
    ]
    for pattern in addr_patterns:
        m = re.search(pattern, text)
        if m:
            addr = m.group(1).strip() if m.lastindex else m.group(0).strip()
            # Sanity check: must be reasonable length and not navigation text
            if 5 < len(addr) < 80 and "dienstverlening" not in addr.lower():
                result["adres"] = addr
                break

    return result
