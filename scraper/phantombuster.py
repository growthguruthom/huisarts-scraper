"""PhantomBuster API integration for LinkedIn Sales Navigator results.

Fetches results from PhantomBuster's API and creates signals for
practice manager changes and other LinkedIn-detected triggers.

Setup:
1. Create a Sales Navigator Search Export phantom in PhantomBuster
2. Get your API key from PhantomBuster Settings -> API Keys
3. Get the Phantom ID from the URL: phantombuster.com/automations/.../XXXXX/...
4. Add to .env:
   PHANTOMBUSTER_API_KEY=your_api_key
   PHANTOMBUSTER_AGENT_IDS=12345,67890  (comma-separated if multiple phantoms)
"""

import csv
import io
import json
import logging
import os
import re
import time

import requests

from scraper.db import insert_signaal

logger = logging.getLogger(__name__)

PB_API_BASE = 'https://api.phantombuster.com/api/v2'

GP_KEYWORDS = [
    'huisarts', 'huisartsen', 'gezondheidscentrum', 'medisch centrum',
    'health center', 'eerstelijn', 'zorgcentrum', 'praktijk', 'dokter',
    'doktersdienst',
]

RELEVANT_TITLES = [
    'praktijkmanager', 'office manager', 'practice manager',
    'praktijkhouder', 'eigenaar', 'directeur', 'bestuurder',
    'huisarts', 'manager', 'coördinator', 'coordinator',
]



def fetch_phantombuster(agent_ids: str = None, api_key: str = None) -> int:
    """Fetch latest results from PhantomBuster agents and create signals.

    Args:
        agent_ids: Comma-separated PhantomBuster agent/phantom IDs.
                   Falls back to PHANTOMBUSTER_AGENT_IDS env var.
        api_key: PhantomBuster API key.
                 Falls back to PHANTOMBUSTER_API_KEY env var.

    Returns:
        Number of new signals created.
    """
    api_key = api_key or os.getenv('PHANTOMBUSTER_API_KEY')
    agent_ids_str = agent_ids or os.getenv('PHANTOMBUSTER_AGENT_IDS', '')

    if not api_key:
        print('PhantomBuster overgeslagen: geen API key geconfigureerd.')
        print('  Configureer PHANTOMBUSTER_API_KEY in .env')
        return 0

    if not agent_ids_str:
        print('PhantomBuster overgeslagen: geen Agent IDs geconfigureerd.')
        print('  Configureer PHANTOMBUSTER_AGENT_IDS in .env')
        return 0

    ids = [x.strip() for x in agent_ids_str.split(',') if x.strip()]
    print(f'PhantomBuster: resultaten ophalen van {len(ids)} phantom(s)...')

    headers = {
        'X-Phantombuster-Key': api_key,
        'Accept': 'application/json',
    }

    total_new = 0

    for agent_id in ids:
        print(f'\n  Phantom {agent_id}:')
        new = _process_agent(agent_id, headers)
        total_new += new

    print(f'\nPhantomBuster klaar: {total_new} nieuwe signalen.')
    return total_new


def _process_agent(agent_id: str, headers: dict) -> int:
    """Fetch and process results from a single PhantomBuster agent."""
    try:
        resp = requests.get(
            f'{PB_API_BASE}/agents/fetch',
            params={'id': agent_id},
            headers=headers,
            timeout=15,
        )
        resp.raise_for_status()
        agent_data = resp.json()
    except requests.RequestException as e:
        print(f'    API fout: {e}')
        return 0

    s3_folder = agent_data.get('s3Folder')
    result_object = agent_data.get('lastEndMessage')
    agent_name = agent_data.get('name', agent_id)

    print(f'    Naam: {agent_name}')

    # Try to find results from output
    leads = []

    try:
        output_resp = requests.get(
            f'{PB_API_BASE}/agents/fetch-output',
            params={'id': agent_id},
            headers=headers,
            timeout=15,
        )

        if output_resp.status_code == 200:
            output_data = output_resp.json()
            output_text = output_data.get('output', '')

            # Look for JSON/CSV URLs in output
            import re
            json_match = re.search(r'(https://phantombuster\.s3\.amazonaws\.com/.+?\.json)\s*$', output_text, re.MULTILINE)
            csv_match = re.search(r'(https://phantombuster\.s3\.amazonaws\.com/.+?\.csv)\s*$', output_text, re.MULTILINE)

            if json_match:
                leads = _fetch_json_results(json_match.group(1))
            if not leads and csv_match:
                leads = _fetch_csv_results(csv_match.group(1))
    except Exception as e:
        logger.debug(f'Output parsing fout: {e}')

    # Try S3 folder fallback
    if not leads and s3_folder:
        for base in ('https://phantombuster.s3.amazonaws.com', 'https://cache1.phantombuster.com', 'https://cache2.phantombuster.com'):
            org_folder = s3_folder.split('/')[0] if '/' in s3_folder else ''
            json_url = f'{base}/{s3_folder}/result.json'
            leads = _fetch_json_results(json_url)
            if leads:
                break

    # Try fetch-output endpoint as last resort
    if not leads:
        leads = _fetch_output(agent_id, headers)

    if not leads:
        print('    Geen resultaten gevonden')
        return 0

    print(f'    {len(leads)} profielen gevonden')

    new_count = 0
    skipped = 0

    for lead in leads:
        # Normalize field names
        normalized = _normalize_fields(lead)

        name = normalized.get('name', '').strip()
        title = normalized.get('title', '').strip()
        company = normalized.get('company', '').strip()

        if not name or not company:
            continue

        # Check if it's a GP practice
        if not _is_gp_practice(company):
            skipped += 1
            continue

        # Check if the title is relevant
        if title and not _is_relevant_title(title):
            skipped += 1
            continue

        # Classify the trigger
        trigger_type = _classify_trigger(title)

        # Build signal title
        if title:
            signal_title = f'{name} \u2014 {title} bij {company}'
        else:
            signal_title = f'{name} \u2014 bij {company}'

        desc_parts = []
        if title:
            desc_parts.append(f'Functie: {title}')
        desc_parts.append(f'Organisatie: {company}')
        if normalized.get('location'):
            desc_parts.append(f'Locatie: {normalized["location"]}')
        if normalized.get('company_url'):
            desc_parts.append(f'Website: {normalized["company_url"]}')
        if normalized.get('profile_url'):
            desc_parts.append(f'LinkedIn: {normalized["profile_url"]}')
        description = ' | '.join(desc_parts)

        # Build bron_url
        bron_url = normalized.get('profile_url') or f'phantombuster://{name}/{company}'.lower().replace(' ', '-')

        result = insert_signaal(
            type=trigger_type,
            titel=signal_title,
            omschrijving=description,
            bron_url=bron_url,
        )

        if result is None:
            continue
        new_count += 1

    print(f'    \u2192 {new_count} nieuwe signalen, {skipped} niet-relevant overgeslagen')
    return new_count



def _fetch_json_results(url: str) -> list:
    """Fetch JSON result file from PhantomBuster S3."""
    try:
        from urllib.parse import quote
        url = quote(url, safe=':/?=&')
        resp = requests.get(url, timeout=15)
        if resp.status_code != 200:
            logger.debug(f'JSON fetch {resp.status_code}: {url[:80]}')
            return []
        data = resp.json()
        if isinstance(data, list):
            return data
        return [data]
    except Exception as e:
        logger.debug(f'JSON fetch fout: {e}')
        return []


def _fetch_csv_results(url: str) -> list:
    """Fetch CSV result file from PhantomBuster S3."""
    try:
        from urllib.parse import quote
        url = quote(url, safe=':/?=&')
        resp = requests.get(url, timeout=15)
        if resp.status_code != 200:
            logger.debug(f'CSV fetch {resp.status_code}: {url[:80]}')
            return []
        reader = csv.DictReader(io.StringIO(resp.text))
        return list(reader)
    except Exception as e:
        logger.debug(f'CSV fetch fout: {e}')
        return []


def _fetch_output(agent_id: str, headers: dict) -> list:
    """Fetch results via /agents/fetch-output endpoint."""
    try:
        resp = requests.get(
            f'{PB_API_BASE}/agents/fetch-output',
            params={'id': agent_id},
            headers=headers,
            timeout=15,
        )
        if resp.status_code != 200:
            return []
        data = resp.json()

        # Try to parse resultObject
        result_obj = data.get('resultObject')
        if result_obj:
            if isinstance(result_obj, str):
                result_obj = json.loads(result_obj)
            if isinstance(result_obj, list):
                return result_obj
            return [result_obj]
    except Exception as e:
        logger.debug(f'fetch-output fout: {e}')

    return []


def _normalize_fields(lead: dict) -> dict:
    """Normalize PhantomBuster field names to standard format."""
    result = {}

    # Name: try multiple field names
    result['name'] = (
        lead.get('fullName', '') or
        lead.get('full_name', '') or
        f"{lead.get('firstName', '')} {lead.get('lastName', '')}".strip() or
        lead.get('name', '')
    )

    # Title: try multiple field names
    result['title'] = (
        lead.get('jobTitle', '') or
        lead.get('title', '') or
        lead.get('job', '') or
        lead.get('currentJobTitle', '')
    )

    # Company: try multiple field names
    result['company'] = (
        lead.get('companyName', '') or
        lead.get('company', '') or
        lead.get('currentCompanyName', '')
    )

    # Location
    result['location'] = (
        lead.get('location', '') or
        lead.get('city', '') or
        lead.get('region', '')
    )

    # Profile URL
    result['profile_url'] = (
        lead.get('profileUrl', '') or
        lead.get('linkedInProfileUrl', '') or
        lead.get('salesNavigatorUrl', '') or
        lead.get('vmid', '')
    )

    # Fix relative profile URLs
    if result['profile_url'] and not result['profile_url'].startswith('http'):
        if result['profile_url'].startswith('/'):
            result['profile_url'] = f'https://www.linkedin.com{result["profile_url"]}'

    # Company URL
    result['company_url'] = (
        lead.get('companyUrl', '') or
        lead.get('companyWebsite', '')
    )

    return result


def _is_gp_practice(company: str) -> bool:
    """Check if company name indicates a GP practice or health center."""
    company_lower = company.lower()
    return any(kw in company_lower for kw in GP_KEYWORDS)


def _is_relevant_title(title: str) -> bool:
    """Check if job title indicates a key decision maker."""
    title_lower = title.lower()
    return any(kw in title_lower for kw in RELEVANT_TITLES)


def _classify_trigger(title: str) -> str:
    """Classify a LinkedIn profile into a trigger type based on job title."""
    title_lower = (title or '').lower()

    if any(kw in title_lower for kw in ('praktijkmanager', 'office manager', 'practice manager', 'manager')):
        return 'nieuwe_manager'

    if any(kw in title_lower for kw in ('praktijkhouder', 'eigenaar', 'directeur', 'bestuurder')):
        return 'nieuwe_manager'

    return 'nieuwe_manager'
