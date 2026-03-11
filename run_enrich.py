"""Standalone enrichment script - bypasses get_connection() to avoid PRAGMA locks."""
import sys
sys.stdout.reconfigure(line_buffering=True)

import re
import time
import sqlite3
import requests
from bs4 import BeautifulSoup

DB_PATH = "data/huisarts.db"

def extract_address(text):
    result = {"adres": None, "postcode": None}
    m = re.search(r"\b(\d{4}\s?[A-Z]{2})\b", text)
    if m:
        result["postcode"] = m.group(1).replace(" ", "")
    patterns = [
        r"([A-Z][a-zàáâãäåèéêëìíîïòóôõöùúûüý]+(?:straat|weg|laan|plein|singel|gracht|kade|dijk|dreef|hof|pad|ring|steeg|dam|markt|park|baan|vest|wal)\s+\d+[a-zA-Z]?(?:\s*(?:en|tot en met|t/m|-)\s*\d+[a-zA-Z]?)?)",
        r"(?:gelegen\s+(?:aan|op|bij|te)|ter hoogte van|ter plaatse van)\s+(?:de\s+)?([A-Z][a-zàáâãäåèéêëìíîïòóôõöùúûüý]+(?:\s+[a-zàáâãäåèéêëìíîïòóôõöùúûüý]+)*\s+\d+[a-zA-Z]?)",
    ]
    for pattern in patterns:
        m = re.search(pattern, text)
        if m:
            addr = m.group(1).strip() if m.lastindex else m.group(0).strip()
            if 5 < len(addr) < 80 and "dienstverlening" not in addr.lower():
                result["adres"] = addr
                break
    return result

# Read with read-only connection
print("Lezen van signalen...")
ro = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True, timeout=10)
ro.row_factory = sqlite3.Row
rows = ro.execute("""
    SELECT id, titel, bron_url FROM signalen
    WHERE type = 'omgevingsvergunning'
    AND adres IS NULL AND bron_url IS NOT NULL
""").fetchall()
ro.close()

signalen = [dict(r) for r in rows]
print(f"Gevonden: {len(signalen)} signalen zonder adres")

if not signalen:
    print("Niets te doen.")
    sys.exit(0)

# Phase 1: titles
print("Fase 1: adressen uit titels...")
title_updates = []
remaining = []
for s in signalen:
    result = extract_address(s.get("titel") or "")
    if result["adres"] or result["postcode"]:
        title_updates.append((result["adres"], result["postcode"], s["id"]))
    else:
        remaining.append(s)

if title_updates:
    wc = sqlite3.connect(DB_PATH, timeout=30)
    wc.executemany("UPDATE signalen SET adres = ?, postcode = ? WHERE id = ?", title_updates)
    wc.commit()
    wc.close()
    print(f"  {len(title_updates)} adressen uit titels")

# Phase 2: detail pages
print(f"Fase 2: {len(remaining)} detail-pagina's ophalen...")
session = requests.Session()
session.headers.update({"User-Agent": "HuisartsScraper/1.0", "Accept": "text/html"})

page_updates = []
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
        result = extract_address(text)
        if result["adres"] or result["postcode"]:
            page_updates.append((result["adres"], result["postcode"], s["id"]))
    except Exception as e:
        print(f"  Fout: {e}")

    if (i + 1) % 10 == 0:
        # Batch commit every 10
        if page_updates:
            wc = sqlite3.connect(DB_PATH, timeout=30)
            wc.executemany("UPDATE signalen SET adres = ?, postcode = ? WHERE id = ?", page_updates)
            wc.commit()
            wc.close()
        print(f"  {i+1}/{len(remaining)} pagina's ({len(page_updates)} adressen)")
        page_updates = []

    time.sleep(1.5)

# Final commit
if page_updates:
    wc = sqlite3.connect(DB_PATH, timeout=30)
    wc.executemany("UPDATE signalen SET adres = ?, postcode = ? WHERE id = ?", page_updates)
    wc.commit()
    wc.close()

total_titles = len(title_updates)
total_pages = sum(1 for _ in []) # already committed in batches
print(f"Klaar! {total_titles} uit titels + pagina-adressen verrijkt.")
