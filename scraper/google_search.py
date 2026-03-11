"""Search Google Custom Search for news about GP practice renovations."""

import os
import requests
from scraper.db import insert_signaal

SEARCH_QUERIES = [
    '"verbouwing huisartsenpraktijk" 2026',
    '"nieuwbouw huisarts" nederland 2026',
    '"huisartsenpraktijk" "nieuwe locatie" 2026',
    '"huisarts" verbouwing nieuwbouw site:skipr.nl OR site:cobouw.nl OR site:medischcontact.nl',
]

CSE_URL = "https://www.googleapis.com/customsearch/v1"


def scrape_google(api_key: str = None, cse_id: str = None) -> int:
    """Search Google Custom Search for GP renovation news.

    Args:
        api_key: Google API key. Falls back to GOOGLE_API_KEY env var.
        cse_id: Custom Search Engine ID. Falls back to GOOGLE_CSE_ID env var.
    """
    api_key = api_key or os.getenv("GOOGLE_API_KEY")
    cse_id = cse_id or os.getenv("GOOGLE_CSE_ID")

    if not api_key or not cse_id:
        print("Google Search overgeslagen: geen API key of CSE ID geconfigureerd.")
        print("  Configureer GOOGLE_API_KEY en GOOGLE_CSE_ID in .env")
        return 0

    print("Google Custom Search: nieuwsberichten zoeken...")
    total_new = 0

    for query in SEARCH_QUERIES:
        print(f"  Query: {query}")
        new = _search_query(api_key, cse_id, query)
        total_new += new
        print(f"    → {new} nieuwe resultaten")

    print(f"Klaar: {total_new} nieuwe nieuwsberichten opgeslagen.")
    return total_new


def _search_query(api_key: str, cse_id: str, query: str) -> int:
    """Execute a single search query and store results."""
    new_count = 0

    # Fetch up to 3 pages (30 results) per query
    for start in range(1, 31, 10):
        params = {
            "key": api_key,
            "cx": cse_id,
            "q": query,
            "start": start,
            "num": 10,
            "lr": "lang_nl",
            "gl": "nl",
        }

        try:
            resp = requests.get(CSE_URL, params=params, timeout=15)
            if resp.status_code == 429:
                print("    Rate limit bereikt, stoppen met Google search.")
                return new_count
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException as e:
            print(f"    Google API fout: {e}")
            break

        items = data.get("items", [])
        if not items:
            break

        for item in items:
            result = insert_signaal(
                type="nieuwsbericht",
                titel=item.get("title", ""),
                omschrijving=item.get("snippet", ""),
                bron_url=item.get("link", ""),
                publicatiedatum=item.get("pagemap", {}).get("metatags", [{}])[0].get("article:published_time", "")[:10] if item.get("pagemap") else None,
            )
            if result is not None:
                new_count += 1

        # Stop if fewer results than requested (no more pages)
        total_results = int(data.get("searchInformation", {}).get("totalResults", 0))
        if start + 10 > total_results:
            break

    return new_count
