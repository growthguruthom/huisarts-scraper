"""LinkedIn Sales Navigator integration for detecting practice manager changes.

Uses LinkedIn Sales Navigator saved search exports to detect:
- New practice managers at GP practices (trigger: nieuwe_manager)
- Role changes at healthcare organizations
- Practice mergers (company page updates)

Integration methods:
1. CSV export from Sales Navigator saved searches (manual, but free)
2. PhantomBuster automation (paid, ~$59/mo)
3. Clay.com enrichment (paid, automated)

Setup instructions:
1. In Sales Navigator, create these saved searches:

   SEARCH 1: "Nieuwe Praktijkmanagers"
   - Title: praktijkmanager
   - Industry: Hospital & Health Care
   - Geography: Netherlands
   - Changed jobs: Past 90 days
   \u2192 Save search & enable alerts

   SEARCH 2: "Praktijkhouders Huisarts"
   - Title: praktijkhouder OR huisarts
   - Industry: Hospital & Health Care
   - Geography: Netherlands
   - Changed jobs: Past 90 days
   \u2192 Save search & enable alerts

2. Export leads to CSV weekly
3. Place CSV in data/linkedin/ folder
4. Run: python main.py --import-linkedin

The CSV parser below handles both Sales Navigator exports
and PhantomBuster output formats.
"""

import csv
import re
import logging
from pathlib import Path
from datetime import datetime

from scraper.db import insert_signaal, get_connection

logger = logging.getLogger(__name__)

LINKEDIN_DIR = Path(__file__).parent.parent / 'data' / 'linkedin'

RELEVANT_TITLES = [
    'praktijkmanager', 'office manager', 'praktijkhouder', 'huisarts',
    'manager', 'directeur', 'eigenaar', 'bestuurder',
]

RELEVANT_COMPANIES = [
    'huisarts', 'gezondheidscentrum', 'medisch centrum', 'health center',
    'huisartsen', 'eerstelijn', 'zorgcentrum',
]


def import_linkedin_csv(filepath: str = None) -> int:
    """Import LinkedIn Sales Navigator CSV export and create signals.

Args:
    filepath: Path to CSV file. If None, processes all CSVs in data/linkedin/

Returns:
    Number of new signals created.
"""
    LINKEDIN_DIR.mkdir(parents=True, exist_ok=True)

    files = []
    if filepath:
        files = [Path(filepath)]
    else:
        files = sorted(LINKEDIN_DIR.glob('*.csv'))

    if not files:
        print('Geen LinkedIn CSV bestanden gevonden in data/linkedin/')
        print('  Exporteer leads uit Sales Navigator en plaats de CSV hier.')
        return 0

    total_new = 0
    for csv_file in files:
        print(f'  Verwerken: {csv_file.name}')
        new = _process_csv(csv_file)
        total_new += new
        if new > 0:
            print(f'    \u2192 {new} nieuwe signalen')

        processed_dir = LINKEDIN_DIR / 'processed'
        processed_dir.mkdir(exist_ok=True)
        dest = processed_dir / f'{csv_file.stem}_{datetime.now().strftime("%Y%m%d")}{csv_file.suffix}'
        csv_file.rename(dest)

    print(f'LinkedIn import klaar: {total_new} nieuwe signalen.')
    return total_new


def _process_csv(csv_file: Path) -> int:
    """Process a single LinkedIn CSV export file."""
    new_count = 0

    try:
        with open(csv_file, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            headers = reader.fieldnames or []

            format_type = _detect_format(headers)

            for row in reader:
                signal = _parse_row(row, format_type)
                if not signal:
                    continue

                if not _is_relevant_company(signal.get('company', '')):
                    continue

                signal_type = _classify_linkedin_signal(signal)

                title = _build_signal_title(signal)
                description = _build_signal_description(signal)

                bron_url = signal.get('profile_url', '')
                if not bron_url:
                    bron_url = f"linkedin://{signal.get('name', 'unknown')}/{signal.get('company', '')}".lower().replace(' ', '-')

                result = insert_signaal(
                    type=signal_type,
                    titel=title,
                    omschrijving=description,
                    bron_url=bron_url,
                )
                if result is None:
                    continue
                new_count += 1

    except Exception as e:
        logger.warning(f'Fout bij verwerken {csv_file}: {e}')
        print(f'    Fout: {e}')

    return new_count


def _detect_format(headers: list[str]) -> str:
    """Detect CSV format from headers."""
    headers_lower = [h.lower() for h in headers]

    if 'first name' in headers_lower and 'last name' in headers_lower:
        return 'sales_navigator'
    if 'fullname' in headers_lower or 'full name' in headers_lower:
        return 'phantombuster'
    if 'naam' in headers_lower or 'name' in headers_lower:
        return 'generic'
    return 'generic'


def _parse_row(row: dict, format_type: str) -> dict | None:
    """Parse a CSV row into a normalized dict."""
    normalized = {k.lower().strip(): v.strip() if v else '' for k, v in row.items()}

    result = {}

    if format_type == 'sales_navigator':
        result['name'] = f"{normalized.get('first name', '')} {normalized.get('last name', '')}".strip()
        result['title'] = normalized.get('title', '') or normalized.get('job title', '')
        result['company'] = normalized.get('company', '') or normalized.get('company name', '')
        result['location'] = normalized.get('geography', '') or normalized.get('location', '')
        result['profile_url'] = normalized.get('linkedin url', '') or normalized.get('profile url', '')
        result['connected_on'] = normalized.get('connected on', '')

    elif format_type == 'phantombuster':
        result['name'] = normalized.get('fullname', '') or normalized.get('full name', '')
        result['title'] = normalized.get('title', '') or normalized.get('jobtitle', '')
        result['company'] = normalized.get('companyname', '') or normalized.get('company', '')
        result['location'] = normalized.get('location', '')
        result['profile_url'] = normalized.get('profileurl', '') or normalized.get('linkedin url', '')

    else:
        result['name'] = normalized.get('naam', '') or normalized.get('name', '')
        result['title'] = normalized.get('titel', '') or normalized.get('title', '') or normalized.get('functie', '')
        result['company'] = normalized.get('bedrijf', '') or normalized.get('company', '')
        result['location'] = normalized.get('locatie', '') or normalized.get('location', '')
        result['profile_url'] = normalized.get('linkedin', '') or normalized.get('url', '')

    if not result.get('name') or not result.get('company'):
        return None

    return result


def _is_relevant_company(company: str) -> bool:
    company_lower = company.lower()
    return any(kw in company_lower for kw in RELEVANT_COMPANIES)


def _classify_linkedin_signal(signal: dict) -> str:
    title = (signal.get('title') or '').lower()

    if any(kw in title for kw in ('praktijkmanager', 'office manager', 'manager')):
        return 'nieuwe_manager'

    if any(kw in title for kw in ('praktijkhouder', 'eigenaar', 'partner', 'directeur')):
        return 'nieuwe_manager'

    return 'nieuwe_manager'


def _build_signal_title(signal: dict) -> str:
    name = signal.get('name', 'Onbekend')
    title = signal.get('title', '')
    company = signal.get('company', '')

    if title:
        return f'{name} \u2014 {title} bij {company}'
    return f'{name} \u2014 nieuwe rol bij {company}'


def _build_signal_description(signal: dict) -> str:
    parts = []
    if signal.get('title'):
        parts.append(f"Functie: {signal['title']}")
    if signal.get('company'):
        parts.append(f"Organisatie: {signal['company']}")
    if signal.get('location'):
        parts.append(f"Locatie: {signal['location']}")
    if signal.get('profile_url'):
        parts.append(f"LinkedIn: {signal['profile_url']}")
    return ' | '.join(parts)
