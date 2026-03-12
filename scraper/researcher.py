"""Research enrichment: find contact persons and news articles for matched practices."""

import json
import logging
import re
import time
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from scraper.db import get_matches_without_research, insert_research, get_connection

logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": "HuisartsScraper/1.0 (lead-generation tool)",
    "Accept": "text/html,application/xhtml+xml",
}

# Keywords to find team/about pages
TEAM_KEYWORDS = [
    "team", "over-ons", "over ons", "medewerkers", "wie-zijn-wij",
    "wie zijn wij", "ons-team", "onze-huisartsen", "huisartsen",
    "praktijkhouder", "artsen", "specialisten", "organisatie",
]

# Keywords to find news/blog pages
NEWS_KEYWORDS = [
    "nieuws", "blog", "actueel", "bericht", "update", "mededelingen",
    "verbouwing", "nieuwbouw", "verhuizing", "bouw",
]


def research_matches(delay: float = 2.0):
    """Run research enrichment on all unresearched matches with a practice website."""
    matches = get_matches_without_research()
    if not matches:
        print("Geen matches om te onderzoeken.")
        return

    print(f"Research starten voor {len(matches)} matches...")

    try:
        import anthropic
    except ImportError:
        print("FOUT: anthropic package niet geinstalleerd. Run: pip install anthropic")
        return

    client = anthropic.Anthropic()
    session = requests.Session()
    session.headers.update(HEADERS)

    researched = 0
    for i, match in enumerate(matches):
        try:
            result = _research_single_match(client, session, match, delay)
            if result:
                insert_research(match["match_id"], result)
                researched += 1
                name = result.get("contact_naam") or "-"
                news = result.get("nieuws_titel") or "-"
                print(f"  [{i+1}/{len(matches)}] {match['praktijk_naam']}: contact={name}, nieuws={news}")
            else:
                print(f"  [{i+1}/{len(matches)}] {match['praktijk_naam']}: geen resultaten")
        except Exception as e:
            logger.warning(f"Research fout voor {match['praktijk_naam']}: {e}")
            print(f"  [{i+1}/{len(matches)}] {match['praktijk_naam']}: FOUT - {e}")

    print(f"Research klaar: {researched}/{len(matches)} matches verrijkt.")


def _research_single_match(client, session: requests.Session, match: dict, delay: float) -> dict | None:
    """Research a single match: scrape website, then ask Claude to extract info."""
    website = (match.get("website") or "").strip()

    # If no website stored, try to find it via Google
    if not website:
        website = _find_practice_website(session, match["praktijk_naam"], match.get("praktijk_stad", ""))
        if website:
            # Save discovered website to praktijken table for future use
            _save_website(match.get("agb_code"), website)
        time.sleep(delay)

    if not website:
        return None

    # Ensure URL has protocol
    if not website.startswith("http"):
        website = "https://" + website

    # Step 1: Scrape the practice website for relevant pages
    pages = _scrape_practice_website(session, website, delay)
    if not pages or not pages.get("homepage"):
        return None

    # Step 2: Send to Claude for extraction
    return _claude_extract(client, match, pages)


def _find_practice_website(session: requests.Session, naam: str, stad: str) -> str | None:
    """Try to find the practice website by searching Google."""
    import os
    api_key = os.environ.get("GOOGLE_API_KEY")
    cse_id = os.environ.get("GOOGLE_CSE_ID")
    if not api_key or not cse_id:
        return None

    query = f"{naam} {stad} huisarts"
    try:
        resp = session.get(
            "https://www.googleapis.com/customsearch/v1",
            params={"key": api_key, "cx": cse_id, "q": query, "num": 3},
            timeout=10,
        )
        if resp.status_code != 200:
            return None

        results = resp.json().get("items", [])
        # Look for a likely practice website (not zorgkaart, google, etc.)
        skip_domains = ["zorgkaartnederland.nl", "google.", "facebook.", "linkedin.", "instagram."]
        for item in results:
            url = item.get("link", "")
            if not any(d in url.lower() for d in skip_domains):
                return url
    except Exception:
        pass
    return None


def _save_website(agb_code: str, website: str):
    """Save discovered website URL to praktijken table."""
    if not agb_code:
        return
    try:
        conn = get_connection()
        conn.execute("UPDATE praktijken SET website = ? WHERE agb_code = ? AND (website IS NULL OR website = '')",
                     (website, agb_code))
        conn.commit()
        conn.close()
    except Exception:
        pass


def _scrape_practice_website(session: requests.Session, base_url: str, delay: float) -> dict:
    """Scrape practice website to find team and news pages.

    Returns dict with 'homepage', 'team_pages', 'news_pages' text content.
    """
    result = {"homepage": "", "team_pages": [], "news_pages": []}

    # Fetch homepage
    try:
        resp = session.get(base_url, timeout=15, allow_redirects=True)
        if resp.status_code != 200:
            return result
        soup = BeautifulSoup(resp.text, "lxml")
        result["homepage"] = _extract_text(soup, max_chars=2000)

        # Find all links on homepage
        links = _extract_links(soup, base_url)
    except Exception as e:
        logger.debug(f"Homepage fout {base_url}: {e}")
        return result

    # Categorize links
    team_urls = []
    news_urls = []

    for url, text in links:
        url_lower = url.lower()
        text_lower = text.lower()
        combined = url_lower + " " + text_lower

        if any(kw in combined for kw in TEAM_KEYWORDS):
            team_urls.append(url)
        if any(kw in combined for kw in NEWS_KEYWORDS):
            news_urls.append(url)

    # Fetch team pages (max 2)
    for url in team_urls[:2]:
        time.sleep(delay)
        text = _fetch_page_text(session, url)
        if text:
            result["team_pages"].append({"url": url, "text": text})

    # Fetch news pages (max 2)
    for url in news_urls[:2]:
        time.sleep(delay)
        text = _fetch_page_text(session, url)
        if text:
            result["news_pages"].append({"url": url, "text": text})

    return result


def _extract_links(soup: BeautifulSoup, base_url: str) -> list[tuple[str, str]]:
    """Extract all internal links from a page."""
    base_domain = urlparse(base_url).netloc
    links = []

    for a in soup.find_all("a", href=True):
        href = a["href"]
        full_url = urljoin(base_url, href)
        parsed = urlparse(full_url)

        # Only internal links
        if parsed.netloc != base_domain:
            continue
        # Skip anchors, files, mailto
        if parsed.path.endswith((".pdf", ".jpg", ".png", ".zip")):
            continue
        if href.startswith(("mailto:", "tel:", "#")):
            continue

        link_text = a.get_text(strip=True)
        links.append((full_url, link_text))

    return links


def _fetch_page_text(session: requests.Session, url: str, max_chars: int = 3000) -> str | None:
    """Fetch and extract text from a single page."""
    try:
        resp = session.get(url, timeout=15)
        if resp.status_code != 200:
            return None
        soup = BeautifulSoup(resp.text, "lxml")
        return _extract_text(soup, max_chars)
    except Exception:
        return None


def _extract_text(soup: BeautifulSoup, max_chars: int = 3000) -> str:
    """Extract main text content from HTML, removing nav/header/footer."""
    # Remove non-content elements
    for tag in soup.find_all(["nav", "header", "footer", "script", "style", "noscript"]):
        tag.decompose()

    # Try to find main content area
    main = soup.find("main") or soup.find(id="content") or soup.find(class_="content")
    if main:
        text = main.get_text(" ", strip=True)
    else:
        text = soup.get_text(" ", strip=True)

    # Clean up whitespace
    text = re.sub(r"\s+", " ", text).strip()
    return text[:max_chars]


def _claude_extract(client, match: dict, pages: dict) -> dict | None:
    """Use Claude to extract contact person and news from scraped pages."""
    # Build context from scraped pages
    context_parts = []

    if pages.get("homepage"):
        context_parts.append(f"=== HOMEPAGE ===\n{pages['homepage'][:1500]}")

    for p in pages.get("team_pages", []):
        context_parts.append(f"=== TEAMPAGINA ({p['url']}) ===\n{p['text'][:2000]}")

    for p in pages.get("news_pages", []):
        context_parts.append(f"=== NIEUWSPAGINA ({p['url']}) ===\n{p['text'][:2000]}")

    if not context_parts:
        return None

    website_content = "\n\n".join(context_parts)

    prompt = f"""Je bent een research assistent die huisartspraktijken onderzoekt.

Praktijk: {match['praktijk_naam']}
Stad: {match.get('praktijk_stad', 'onbekend')}
Gemeente: {match.get('gemeente', 'onbekend')}
Signaal: {match.get('signaal_titel', 'verbouwing/omgevingsvergunning')}

Hieronder staat de inhoud van de praktijkwebsite:

{website_content}

Zoek de volgende informatie:

1. CONTACTPERSOON: Wie is de praktijkhouder of praktijkmanager? Geef naam en rol.
   - Prioriteit: praktijkhouder > praktijkmanager > andere leidinggevende
   - Als er meerdere praktijkhouders zijn, kies degene die het meest relevant lijkt

2. NIEUWS OVER VERBOUWING: Is er een nieuwsbericht of blog over een verbouwing, nieuwbouw, verhuizing of bouw?
   - Geef de titel en een korte samenvatting (1-2 zinnen) die als "haakje" gebruikt kan worden voor een outreach.

Geef je antwoord ALLEEN als JSON (geen andere tekst):
{{
    "contact_naam": "naam of null",
    "contact_rol": "praktijkhouder/praktijkmanager/anders of null",
    "contact_bron": "URL van de pagina waar je dit vond of null",
    "nieuws_titel": "titel van het nieuwsbericht of null",
    "nieuws_url": "URL van het nieuwsbericht of null",
    "nieuws_samenvatting": "korte samenvatting als outreach haakje of null"
}}"""

    try:
        response = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=500,
            messages=[{"role": "user", "content": prompt}],
        )

        text = response.content[0].text.strip()

        # Extract JSON from response (handle markdown code blocks)
        json_match = re.search(r"\{[^{}]*\}", text, re.DOTALL)
        if not json_match:
            return None

        data = json.loads(json_match.group())

        # Replace string "null" with actual None
        for key in data:
            if data[key] == "null" or data[key] == "":
                data[key] = None

        # Store raw response
        data["raw_response"] = text

        return data

    except Exception as e:
        logger.warning(f"Claude API fout: {e}")
        return None
