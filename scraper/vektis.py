"""Scrape huisartspraktijken from Vektis AGB register and ZorgkaartNederland."""

import time
import re
import json
import requests
from bs4 import BeautifulSoup
from xml.etree import ElementTree as ET
from tqdm import tqdm
from scraper.db import upsert_praktijk, get_connection

ZORGKAART_SITEMAPS = [
    f"https://www.zorgkaartnederland.nl/files/sitemap/company_{i}.xml"
    for i in range(5)
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "nl-NL,nl;q=0.9",
}


def scrape_zorgkaart(delay: float = 2.0):
    """Scrape all huisartspraktijken from ZorgkaartNederland sitemaps.

    This is the reliable fallback since Vektis has reCAPTCHA protection.
    """
    print("Stap 1: Ophalen van praktijk-URLs uit ZorgkaartNederland sitemaps...")
    urls = _get_practice_urls_from_sitemaps()
    print(f"  Gevonden: {len(urls)} praktijk-URLs")

    print(f"Stap 2: Detail-pagina's ophalen (delay: {delay}s per request)...")
    session = requests.Session()
    session.headers.update(HEADERS)
    count = 0

    for url in tqdm(urls, desc="Praktijken ophalen"):
        try:
            resp = session.get(url, timeout=15)
            if resp.status_code != 200:
                continue
            data = _parse_zorgkaart_detail(resp.text, url)
            if data and data.get("naam"):
                upsert_praktijk(
                    agb_code=data.get("agb_code", f"ZK-{url.split('-')[-1]}"),
                    naam=data["naam"],
                    adres=data.get("adres"),
                    postcode=data.get("postcode"),
                    stad=data.get("stad"),
                    telefoon=data.get("telefoon"),
                    website=data.get("website"),
                    lat=data.get("lat"),
                    lon=data.get("lon"),
                    bron="zorgkaart",
                )
                count += 1
            time.sleep(delay)
        except Exception as e:
            print(f"  Fout bij {url}: {e}")
            continue

    print(f"Klaar: {count} praktijken opgeslagen.")
    return count


def _get_practice_urls_from_sitemaps() -> list[str]:
    """Fetch all huisartsenpraktijk URLs from ZorgkaartNederland sitemaps."""
    urls = []
    session = requests.Session()
    session.headers.update(HEADERS)

    for sitemap_url in ZORGKAART_SITEMAPS:
        try:
            resp = session.get(sitemap_url, timeout=15)
            if resp.status_code != 200:
                print(f"  Kon sitemap niet ophalen: {sitemap_url} (status {resp.status_code})")
                continue
            root = ET.fromstring(resp.content)
            ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
            for loc in root.findall(".//sm:loc", ns):
                url = loc.text.strip()
                if "/zorginstelling/huisartsenpraktijk-" in url:
                    urls.append(url)
        except Exception as e:
            print(f"  Fout bij sitemap {sitemap_url}: {e}")

    return urls


def _parse_zorgkaart_detail(html: str, url: str) -> dict | None:
    """Parse a ZorgkaartNederland practice detail page."""
    soup = BeautifulSoup(html, "lxml")
    data = {}

    # Try Schema.org JSON-LD first
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            ld = json.loads(script.string)
            if isinstance(ld, list):
                ld = ld[0]
            if ld.get("@type") in ("MedicalClinic", "Physician", "LocalBusiness", "MedicalOrganization"):
                data["naam"] = ld.get("name", "")
                addr = ld.get("address", {})
                if isinstance(addr, dict):
                    street = addr.get("streetAddress", "")
                    data["adres"] = street
                    data["postcode"] = addr.get("postalCode", "")
                    data["stad"] = addr.get("addressLocality", "")
                geo = ld.get("geo", {})
                if isinstance(geo, dict):
                    data["lat"] = _safe_float(geo.get("latitude"))
                    data["lon"] = _safe_float(geo.get("longitude"))
                data["telefoon"] = ld.get("telephone", "")
                data["website"] = ld.get("url", "")
                break
        except (json.JSONDecodeError, TypeError):
            continue

    # Fallback: parse HTML directly
    if not data.get("naam"):
        h1 = soup.find("h1")
        if h1:
            data["naam"] = h1.get_text(strip=True)

    if not data.get("adres"):
        addr_el = soup.find("address") or soup.find(class_=re.compile(r"address|adres", re.I))
        if addr_el:
            text = addr_el.get_text(" ", strip=True)
            # Try to parse "Straat 1, 1234 AB Stad"
            m = re.search(r"(.+?),?\s*(\d{4}\s*[A-Z]{2})\s+(.+)", text)
            if m:
                data["adres"] = m.group(1).strip()
                data["postcode"] = m.group(2).strip()
                data["stad"] = m.group(3).strip()

    # Extract coordinates from map iframe or data attributes
    if not data.get("lat"):
        iframe = soup.find("iframe", src=re.compile(r"maps.*[?&]q="))
        if iframe:
            m = re.search(r"[?&]q=([-\d.]+),([-\d.]+)", iframe["src"])
            if m:
                data["lat"] = float(m.group(1))
                data["lon"] = float(m.group(2))

    # Extract ID from URL as pseudo AGB code
    m = re.search(r"-(\d+)$", url)
    data["agb_code"] = f"ZK-{m.group(1)}" if m else None

    return data if data.get("naam") else None


def _safe_float(val) -> float | None:
    try:
        return float(val) if val is not None else None
    except (ValueError, TypeError):
        return None


def scrape_vektis_manual():
    """Open Vektis search in a visible browser for manual CAPTCHA solving.

    This opens a browser window where you can manually solve the CAPTCHA,
    then it scrapes the results.
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print("Playwright niet geïnstalleerd. Gebruik ZorgkaartNederland als alternatief.")
        return 0

    print("Vektis AGB Register openen in browser...")
    print("Let op: los handmatig de CAPTCHA op wanneer deze verschijnt.")

    count = 0
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        page = browser.new_page()
        page.goto("https://www.vektis.nl/agb-register/zoeken")
        page.wait_for_load_state("networkidle")

        # Select "Onderneming/Vestiging" tab for practice-level results
        vestiging_radio = page.locator('input[value="Onderneming/Vestiging"], label:has-text("Onderneming")')
        if vestiging_radio.count() > 0:
            vestiging_radio.first.click()
            time.sleep(1)

        # Select zorgsoort "Huisartsen"
        zorgsoort_dropdown = page.locator('[data-transfer-label="Zorgsoort"]').first
        if zorgsoort_dropdown.count() > 0:
            zorgsoort_dropdown.click()
            time.sleep(0.5)
            page.locator('.dropdown-item:has-text("Huisartsen")').first.click()
            time.sleep(1)

        print("Klik op 'Zoeken' en los de CAPTCHA op in de browser.")
        print("Druk op Enter in de terminal wanneer de resultaten zichtbaar zijn...")
        input()

        # Parse results from the DataTable
        results_html = page.locator(".js-search-results").inner_html()
        soup = BeautifulSoup(results_html, "lxml")

        for row in soup.find_all("tr"):
            cells = row.find_all("td")
            if len(cells) >= 4:
                agb = cells[0].get_text(strip=True)
                naam = cells[1].get_text(strip=True)
                adres_text = cells[2].get_text(strip=True) if len(cells) > 2 else ""
                stad = cells[3].get_text(strip=True) if len(cells) > 3 else ""

                postcode = ""
                m = re.search(r"(\d{4}\s*[A-Z]{2})", adres_text)
                if m:
                    postcode = m.group(1)

                if agb and naam:
                    upsert_praktijk(
                        agb_code=agb,
                        naam=naam,
                        adres=adres_text,
                        postcode=postcode,
                        stad=stad,
                        bron="vektis",
                    )
                    count += 1

        # Check for pagination
        print(f"  Pagina verwerkt: {count} resultaten")
        print("Navigeer handmatig naar volgende pagina's en druk telkens Enter.")
        print("Typ 'klaar' om te stoppen.")

        while True:
            user_input = input("Enter voor volgende pagina, 'klaar' om te stoppen: ")
            if user_input.strip().lower() == "klaar":
                break

            results_html = page.locator(".js-search-results").inner_html()
            soup = BeautifulSoup(results_html, "lxml")
            page_count = 0
            for row in soup.find_all("tr"):
                cells = row.find_all("td")
                if len(cells) >= 4:
                    agb = cells[0].get_text(strip=True)
                    naam = cells[1].get_text(strip=True)
                    adres_text = cells[2].get_text(strip=True) if len(cells) > 2 else ""
                    stad = cells[3].get_text(strip=True) if len(cells) > 3 else ""

                    postcode = ""
                    m = re.search(r"(\d{4}\s*[A-Z]{2})", adres_text)
                    if m:
                        postcode = m.group(1)

                    if agb and naam:
                        upsert_praktijk(
                            agb_code=agb,
                            naam=naam,
                            adres=adres_text,
                            postcode=postcode,
                            stad=stad,
                            bron="vektis",
                        )
                        page_count += 1

            count += page_count
            print(f"  Pagina verwerkt: {page_count} resultaten (totaal: {count})")

        browser.close()

    print(f"Klaar: {count} praktijken opgeslagen via Vektis.")
    return count
