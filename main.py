#!/usr/bin/env python3
"""Huisarts Verbouwing Scraper - CLI entrypoint."""

import argparse
import sys
from datetime import datetime, timedelta
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from dotenv import load_dotenv
load_dotenv(Path(__file__).parent / ".env")

from scraper.db import init_db, get_stats


def main():
    parser = argparse.ArgumentParser(
        description="Identificeer huisartspraktijken die gaan verbouwen in Nederland"
    )
    parser.add_argument(
        "--refresh-practices",
        action="store_true",
        help="Ververs de master list van praktijken (ZorgkaartNederland)"
    )
    parser.add_argument(
        "--refresh-vektis",
        action="store_true",
        help="Ververs praktijken via Vektis (handmatige CAPTCHA vereist)"
    )
    parser.add_argument(
        "--since",
        type=str,
        default=None,
        help="Zoek bekendmakingen sinds datum (YYYY-MM-DD) of relatief (bijv. '7d', '30d', '6m'). Default: 6 maanden geleden"
    )
    parser.add_argument(
        "--enrich",
        action="store_true",
        help="Verrijk signalen met adresgegevens van detail-pagina's"
    )
    parser.add_argument(
        "--export",
        type=str,
        default=None,
        help="Exporteer resultaten naar Excel bestand (bijv. resultaten.xlsx)"
    )
    parser.add_argument(
        "--stats",
        action="store_true",
        help="Toon database statistieken"
    )
    parser.add_argument(
        "--research",
        action="store_true",
        help="Verrijk matches met contactpersoon en nieuwsartikelen via Claude API"
    )
    parser.add_argument(
        "--monitor-news",
        action="store_true",
        help="Monitor Nederlandse zorg-nieuwssites voor signalen (RSS feeds)"
    )
    parser.add_argument(
        "--phantombuster",
        action="store_true",
        help="Haal LinkedIn Sales Navigator resultaten op via PhantomBuster API"
    )
    parser.add_argument(
        "--import-linkedin",
        type=str,
        default=None,
        help="Importeer LinkedIn Sales Navigator CSV export (pad naar bestand)"
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=2.0,
        help="Delay in seconden tussen requests (default: 2.0)"
    )

    args = parser.parse_args()

    # Initialize database
    init_db()

    # Parse since date
    since_date = _parse_since(args.since)

    if args.stats:
        _print_stats()
        return

    # Step 1: Refresh practices if requested
    if args.refresh_practices:
        from scraper.vektis import scrape_zorgkaart
        scrape_zorgkaart(delay=args.delay)

    if args.refresh_vektis:
        from scraper.vektis import scrape_vektis_manual
        scrape_vektis_manual()

    # Step 2: Fetch bekendmakingen
    if not args.refresh_practices and not args.refresh_vektis:
        from scraper.bekendmakingen import scrape_bekendmakingen, enrich_signaal_details
        scrape_bekendmakingen(since=since_date, delay=args.delay)

        # Step 2b: Enrich with detail pages
        if args.enrich:
            enrich_signaal_details(delay=args.delay)

        # Step 3: Google search (optional)
        from scraper.google_search import scrape_google
        scrape_google()

        # Step 3b: Monitor news feeds
        if args.monitor_news:
            from scraper.news_monitor import monitor_news
            monitor_news(since=since_date, delay=args.delay)

        # Step 3c: PhantomBuster LinkedIn signals
        if args.phantombuster:
            from scraper.phantombuster import fetch_phantombuster
            fetch_phantombuster()

        # Step 3d: Import LinkedIn CSV
        if args.import_linkedin:
            from scraper.linkedin_monitor import import_linkedin_csv
            import_linkedin_csv(filepath=args.import_linkedin)

        # Step 4: Match signalen to practices
        from scraper.matcher import match_signalen
        match_signalen(since=since_date)

        # Step 5: Research enrichment (Claude API)
        if args.research:
            from scraper.researcher import research_matches
            research_matches(delay=args.delay)

        # Step 6: CRM cross-check (enrich signals with klant/prospect/lead status)
        from scraper.crm_check import enrich_crm_status
        enrich_crm_status()

    # Step 7: Export if requested
    if args.export:
        from scraper.exporter import export_excel
        export_excel(args.export)

    # Print stats
    _print_stats()


def _parse_since(since_str: str | None) -> str:
    """Parse --since argument to YYYY-MM-DD string."""
    if not since_str:
        # Default: 6 months ago
        return (datetime.now() - timedelta(days=180)).strftime("%Y-%m-%d")

    # Try relative format: 7d, 30d, 6m
    import re
    m = re.match(r"^(\d+)([dm])$", since_str)
    if m:
        num = int(m.group(1))
        unit = m.group(2)
        if unit == "d":
            return (datetime.now() - timedelta(days=num)).strftime("%Y-%m-%d")
        elif unit == "m":
            return (datetime.now() - timedelta(days=num * 30)).strftime("%Y-%m-%d")

    # Try absolute date
    try:
        datetime.strptime(since_str, "%Y-%m-%d")
        return since_str
    except ValueError:
        print(f"Ongeldig datumformaat: {since_str}. Gebruik YYYY-MM-DD of relatief (7d, 30d, 6m).")
        sys.exit(1)


def _print_stats():
    stats = get_stats()
    print("\n--- Database Statistieken ---")
    print(f"  Praktijken:     {stats['praktijken']}")
    print(f"  Signalen:       {stats['signalen']}")
    print(f"  Matches:        {stats['matches']}")
    print(f"  Gemeenten:      {stats['gemeenten']}")
    print(f"  Laatste update: {stats['laatste_update'] or 'nog geen data'}")


if __name__ == "__main__":
    main()
