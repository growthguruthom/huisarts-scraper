"""Cross-reference signals against Zoho CRM data (klanten, prospects, leads).

When a new signal comes in (e.g., new practice manager from PhantomBuster),
this module checks if the practice is already in the CRM:
  - klant     → existing customer (retention approach: "welkom nieuwe PM, wij zijn al partner")
  - prospect  → in pipeline (acceleration: "nieuwe beslisser, kans om door te pakken")
  - lead      → known but not contacted yet (warm outreach: "wij kennen jullie praktijk al")
  - nieuw     → not in CRM (cold outreach opportunity)

Person-level checks (requires Zoho API credentials):
  - Person already in Zoho as contact → moved to NEW practice = SALES signal (they know IDS)
  - New person at existing CUSTOMER practice = SUPPORT signal (new PM, update contact)

Supports two modes:
  - Live: Uses Zoho CRM searchRecords API for Accounts + Contacts (when ZOHO_* env vars set)
  - Offline: Falls back to IDS dashboard embedded-data.js CSV parsing

Usage:
    from scraper.crm_check import enrich_crm_status
    enrich_crm_status()  # Updates all signals with crm_status + crm_signal_type
"""

import csv
import io
import json
import os
import re
import logging
import time
import urllib.request
import urllib.parse
from pathlib import Path
from scraper.db import get_connection

logger = logging.getLogger(__name__)

# Path to the IDS dashboard embedded data
DASHBOARD_DIR = Path(__file__).parent.parent.parent / 'Claude Code' / 'IDS Media Nederland' / 'dashboard'
EMBEDDED_DATA_FILE = DASHBOARD_DIR / 'embedded-data.js'

# Path to CRM export JSON (alternative data source)
CRM_EXPORT_FILE = Path(__file__).parent.parent / 'data' / 'crm_lookup.json'

# Zoho CRM API credentials (from environment)
ZOHO_CLIENT_ID = os.environ.get('ZOHO_CLIENT_ID')
ZOHO_CLIENT_SECRET = os.environ.get('ZOHO_CLIENT_SECRET')
ZOHO_REFRESH_TOKEN = os.environ.get('ZOHO_REFRESH_TOKEN')
ZOHO_API_DOMAIN = os.environ.get('ZOHO_API_DOMAIN', 'https://www.zohoapis.eu')
ZOHO_AUTH_DOMAIN = os.environ.get('ZOHO_AUTH_DOMAIN', 'https://accounts.zoho.eu')

HCP_TYPES = ['Klant - HCP Scherm Abonnement', 'Lead - HCP Scherm Abonnement', 'Prospect - HCP Scherm Abonnement']


# ---------------------------------------------------------------------------
# Zoho API helpers
# ---------------------------------------------------------------------------

def _has_zoho_credentials():
    """Check if Zoho API credentials are available."""
    return all([ZOHO_CLIENT_ID, ZOHO_CLIENT_SECRET, ZOHO_REFRESH_TOKEN])


def _get_access_token():
    """Get a fresh Zoho OAuth access token."""
    url = f"{ZOHO_AUTH_DOMAIN}/oauth/v2/token"
    data = urllib.parse.urlencode({
        'refresh_token': ZOHO_REFRESH_TOKEN,
        'client_id': ZOHO_CLIENT_ID,
        'client_secret': ZOHO_CLIENT_SECRET,
        'grant_type': 'refresh_token',
    }).encode()

    req = urllib.request.Request(url, data=data, method='POST')
    with urllib.request.urlopen(req) as resp:
        result = json.loads(resp.read())

    if 'access_token' not in result:
        raise RuntimeError(f"Failed to get Zoho access token: {result}")

    return result['access_token']


def _zoho_search(access_token, module, criteria=None, word=None, fields=None):
    """Search Zoho CRM module using searchRecords API.

    Args:
        access_token: Zoho OAuth token
        module: "Accounts" or "Contacts"
        criteria: COQL criteria string, e.g. "(Billing_Code:equals:1234AB)"
        word: Free-text word search
        fields: Comma-separated field names to return

    Returns:
        List of matching records, or empty list.
    """
    params = {}
    if criteria:
        params['criteria'] = criteria
    if word:
        params['word'] = word
    if fields:
        params['fields'] = fields

    query_string = urllib.parse.urlencode(params)
    url = f"{ZOHO_API_DOMAIN}/crm/v2/{module}/search?{query_string}"

    req = urllib.request.Request(url)
    req.add_header('Authorization', f"Zoho-oauthtoken {access_token}")

    try:
        with urllib.request.urlopen(req) as resp:
            result = json.loads(resp.read())
        return result.get('data', [])
    except urllib.error.HTTPError as e:
        if e.code == 204:
            return []
        logger.warning(f"Zoho API error ({module}): {e.code}")
        return []


def _search_account_by_name(access_token, name):
    """Search Zoho Accounts by name, filtered to HCP types."""
    results = _zoho_search(
        access_token, 'Accounts', word=name,
        fields='Account_Name,Account_Type,Billing_Code,Billing_City,Billing_Street,Parent_Account')

    return [r for r in results if r.get('Account_Type') in HCP_TYPES]


def _search_account_by_postcode(access_token, postcode):
    """Search Zoho Accounts by postcode."""
    pc = postcode.strip()
    results = _zoho_search(
        access_token, 'Accounts',
        criteria=f"(Billing_Code:equals:{pc})",
        fields='Account_Name,Account_Type,Billing_Code,Billing_City,Billing_Street,Parent_Account')

    # Also try without space
    pc_nospace = pc.replace(' ', '')
    if ' ' in pc:
        results2 = _zoho_search(
            access_token, 'Accounts',
            criteria=f"(Billing_Code:equals:{pc_nospace})",
            fields='Account_Name,Account_Type,Billing_Code,Billing_City,Billing_Street,Parent_Account')

        seen = {r['id'] for r in results}
        results.extend(r for r in results2 if r['id'] not in seen)

    return [r for r in results if r.get('Account_Type') in HCP_TYPES]


def _search_contact_by_name(access_token, full_name):
    """Search Zoho Contacts by person name.

    Returns list of contacts with their associated Account info.
    """
    if not full_name or len(full_name.strip()) < 3:
        return []

    results = _zoho_search(
        access_token, 'Contacts', word=full_name,
        fields='Full_Name,First_Name,Last_Name,Account_Name,Email,Phone,Title')

    return results


def _zoho_record_to_entry(record):
    """Convert a Zoho Account record to our internal format."""
    account_type = record.get('Account_Type', '')
    if 'Klant' in account_type:
        entry_type = 'klant'
    elif 'Prospect' in account_type:
        entry_type = 'prospect'
    else:
        entry_type = 'lead'

    parent = record.get('Parent_Account')
    zorggroep = parent.get('name', '') if isinstance(parent, dict) else ''

    return {
        'name': record.get('Account_Name', ''),
        'type': entry_type,
        'id': record.get('id', ''),
        'stad': record.get('Billing_City', ''),
        'postcode': record.get('Billing_Code', ''),
        'zorggroep': zorggroep,
    }


# ---------------------------------------------------------------------------
# Offline CRM data loading
# ---------------------------------------------------------------------------

def _load_crm_data(embedded_data_path: str = None) -> list[dict]:
    """Load CRM account data from the IDS dashboard embedded-data.js."""
    paths_to_try = []
    if embedded_data_path:
        paths_to_try.append(Path(embedded_data_path))
    paths_to_try.append(EMBEDDED_DATA_FILE)

    home = Path.home()
    paths_to_try.append(home / 'Desktop' / 'Claude Code' / 'IDS Media Nederland' / 'dashboard' / 'embedded-data.js')

    if CRM_EXPORT_FILE.exists():
        with open(CRM_EXPORT_FILE) as f:
            return json.load(f)

    for path in paths_to_try:
        if not path.exists():
            continue
        return _parse_embedded_data(path)

    return []


def _parse_embedded_data(path: Path) -> list[dict]:
    """Parse the IDS dashboard embedded-data.js for CRM accounts."""
    with open(path, 'r') as f:
        content = f.read()

    accounts = []

    klanten_match = re.search(r'const EMBEDDED_KLANTEN_CSV = "(.*?)";', content, re.DOTALL)
    if klanten_match:
        text = klanten_match.group(1).replace('\\r\\n', '\n').strip()
        reader = csv.DictReader(io.StringIO(text))
        for row in reader:
            name = row.get('Klantnaam', '').strip()
            if not name:
                continue
            accounts.append({
                'name': name,
                'type': 'klant',
                'stad': row.get('Postadres (plaats)', ''),
                'postcode': row.get('Postcode (postcode)', ''),
                'contact': row.get('Primary Contact', '').strip(),
            })

    leads_match = re.search(r'const EMBEDDED_LEADS_CSV = "(.*?)";', content, re.DOTALL)
    if leads_match:
        text = leads_match.group(1).replace('\\r\\n', '\n').strip()
        lines = text.split('\n')
        if len(lines) > 2 and 'Record-ID' in lines[2]:
            actual_csv = '\n'.join(lines[2:])
            reader = csv.DictReader(io.StringIO(actual_csv))
            for row in reader:
                name = row.get('Klantnaam', '').strip()
                klant_type = row.get('Klant Type', '').strip().lower()
                if not name:
                    continue
                crm_type = 'prospect' if klant_type == 'prospect' else 'lead'
                accounts.append({
                    'name': name,
                    'type': crm_type,
                    'stad': row.get('Postadres (plaats)', ''),
                    'postcode': row.get('Postcode (postcode)', ''),
                    'contact': row.get('Primary Contact', '').strip(),
                })

    return accounts


# ---------------------------------------------------------------------------
# Name normalization and extraction
# ---------------------------------------------------------------------------

def _normalize_name(name):
    """Normalize a practice name for fuzzy matching."""
    if not name:
        return ''
    name = name.lower().strip()
    # Strip leading/trailing quotes and punctuation first
    name = re.sub(r'^["\'\s]+|["\'\s]+$', '', name)
    # Remove common practice prefixes
    name = re.sub(
        r'^(huisartsenpraktijk|huisartspraktijk|huisartsengroepspraktijk|huisartsen|praktijk|gezondheidscentrum|medisch centrum|apotheek|tandarts|tandartspraktijk|fysiotherapie|diëtistenpraktijk|mc)\s+',
        '', name)
    # Remove BV, B.V., io, i.o. suffixes
    name = re.sub(r'\s+(bv|b\.v\.|io|i\.o\.?)\s*$', '', name, flags=re.IGNORECASE)
    name = re.sub(r'[^\w\s]', ' ', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name


def _extract_company_from_signal(titel, omschrijving):
    """Extract the company/practice name from a signal title or description.

    For LinkedIn/PhantomBuster signals: "Naam — Rol bij Praktijknaam"
    For omgevingsvergunningen: extract actual practice names, not descriptions.
    """
    # Try "bij <company>" pattern in title (LinkedIn signals)
    m = re.search(r'bij\s+(.+?)$', titel, re.IGNORECASE)
    if m:
        candidate = m.group(1).strip()
        # Validate: must look like a practice name, not a generic description
        # Reject things like "huisartsenpraktijk op de gevel" (signage/renovation text)
        lower = candidate.lower()
        reject_phrases = [
            'op de gevel', 'op het terrein', 'aan de', 'in de wijk',
            'naar een', 'tot een', 'met een', 'voor de', 'van de',
            'op het dak', 'aan het', 'in het', 'bij de', 'op de begane',
        ]
        if not any(rp in lower for rp in reject_phrases):
            return candidate

    # Try "Organisatie: <company>" in description
    m = re.search(r'Organisatie:\s*([^|]+)', omschrijving)
    if m:
        return m.group(1).strip()

    # Try common practice name patterns — require a proper name after the prefix
    # Must have at least one capitalized word or recognizable name part
    patterns = [
        r'((?:huisartsenpraktijk|huisartspraktijk)\s+[A-Z][\w\s&-]{2,30})',
        r'(gezondheidscentrum\s+[A-Z][\w\s&-]{2,30})',
        r'(medisch centrum\s+[A-Z][\w\s&-]{2,30})',
    ]
    for pat in patterns:
        m = re.search(pat, titel)
        if m:
            candidate = m.group(1).strip()
            # Strip trailing address parts (numbers, postcodes)
            candidate = re.sub(r'\s+(?:aan|op|te|in|naar|bij)\s+.*$', '', candidate, flags=re.IGNORECASE)
            candidate = re.sub(r'\s+\d{4}\s*[A-Z]{2}\b.*$', '', candidate)  # postcode
            candidate = re.sub(r'\s+\d+\s*$', '', candidate)  # trailing house number
            if len(candidate.split()) >= 2:  # must have prefix + name
                return candidate

    return None


def _extract_person_from_signal(titel, omschrijving):
    """Extract the person name from a signal (PhantomBuster/LinkedIn format).

    Examples:
        "Jan de Vries — Praktijkmanager bij Huisartsenpraktijk De Brug"
        → "Jan de Vries"
    """
    # Try "Name — Role" pattern
    m = re.match(r'^(.+?)\s*[—–-]\s*', titel)
    if m:
        person = m.group(1).strip()
        # Validate: at least 2 words, no digits
        if len(person.split()) >= 2 and not re.search(r'\d', person):
            return person

    # Try "Naam: <name>" in description
    m = re.search(r'Naam:\s*([^|]+)', omschrijving)
    if m:
        return m.group(1).strip()

    return None


# ---------------------------------------------------------------------------
# CRM matching
# ---------------------------------------------------------------------------

STOPWORDS = frozenset({
    'de', 'van', 'het', 'in', 'en', 'te', 'op', 'aan', 'bij', 'voor',
    'met', 'een', 'der', 'den', 'des', 'tot', 'naar', 'uit', 'over',
    'onder', 'door', 'om', 'als',
})


def _find_in_crm(company, name_index):
    """Find a company in the CRM name index using fuzzy matching.

    Matching strategy (in order of confidence):
    1. Exact normalized name match
    2. Substring match (minimum 5 chars to avoid false positives)
    3. Word overlap match (requires significant overlap of meaningful words)
    """
    if not company:
        return None

    company_key = _normalize_name(company)
    if not company_key or len(company_key) < 3:
        return None

    # 1. Exact match
    if company_key in name_index:
        return name_index[company_key]

    # 2. Substring match — strict to avoid false positives
    #    - The contained string must be at least 6 meaningful chars
    #    - Must NOT be a common word that appears in many names
    #    - Must match on a word boundary (not inside another word)
    COMMON_SUBSTRINGS = frozenset({
        'centrum', 'praktijk', 'dokter', 'dokters', 'medisch',
        'noord', 'zuid', 'oost', 'west', 'nieuw',
    })
    for crm_key, account in name_index.items():
        if not crm_key or len(crm_key) < 4:
            continue
        # Check if company_key is contained in crm_key (as a whole word/phrase)
        if company_key in crm_key and len(company_key) >= 6:
            if company_key not in COMMON_SUBSTRINGS:
                # Verify word boundary: the match must start/end at a word boundary
                idx = crm_key.index(company_key)
                at_start = idx == 0 or crm_key[idx - 1] == ' '
                at_end = idx + len(company_key) == len(crm_key) or crm_key[idx + len(company_key)] == ' '
                if at_start and at_end:
                    return account
        # Check if crm_key is contained in company_key (as a whole word/phrase)
        if crm_key in company_key and len(crm_key) >= 6:
            if crm_key not in COMMON_SUBSTRINGS:
                idx = company_key.index(crm_key)
                at_start = idx == 0 or company_key[idx - 1] == ' '
                at_end = idx + len(crm_key) == len(company_key) or company_key[idx + len(crm_key)] == ' '
                if at_start and at_end:
                    return account

    # 3. Word overlap match — strict: needs significant meaningful word overlap
    company_words = set(company_key.split()) - STOPWORDS
    # Remove single-char and numeric-only words
    company_words = {w for w in company_words if len(w) >= 2 and not w.isdigit()}

    if len(company_words) >= 2:  # need at least 2 meaningful words
        best_match = None
        best_overlap = 0
        best_ratio = 0.0
        for crm_key, account in name_index.items():
            crm_words = set(crm_key.split()) - STOPWORDS
            crm_words = {w for w in crm_words if len(w) >= 2 and not w.isdigit()}
            if not crm_words:
                continue
            overlap = len(company_words & crm_words)
            if overlap < 2:  # need at least 2 overlapping meaningful words
                continue
            # Require overlap to cover majority of BOTH sides
            ratio = overlap / max(len(company_words), len(crm_words))
            if ratio < 0.5:
                continue
            if overlap > best_overlap or (overlap == best_overlap and ratio > best_ratio):
                best_overlap = overlap
                best_ratio = ratio
                best_match = account
        if best_match:
            return best_match

    return None


def _find_in_crm_live(company, access_token):
    """Find a company in Zoho CRM via live searchRecords API."""
    if not company:
        return None

    # Strip common prefixes for search
    short = re.sub(
        r'^(huisartsenpraktijk|huisartspraktijk|gezondheidscentrum|medisch centrum)\s+',
        '', company, flags=re.IGNORECASE)

    search_term = short if len(short) >= 3 else company

    results = _search_account_by_name(access_token, search_term)
    if not results:
        return None

    # Find best matching record
    company_norm = _normalize_name(company)
    for record in results:
        record_norm = _normalize_name(record.get('Account_Name', ''))
        if not record_norm:
            continue

        # Exact or substring match
        if record_norm == company_norm or record_norm in company_norm or company_norm in record_norm:
            return _zoho_record_to_entry(record)

        # Word overlap match
        c_words = set(company_norm.split()) - frozenset({'de', 'van', 'het', 'in', 'en'})
        r_words = set(record_norm.split()) - frozenset({'de', 'van', 'het', 'in', 'en'})
        if not c_words:
            continue
        if not r_words:
            continue
        overlap = len(c_words & r_words)
        if not overlap >= 1:
            continue
        if not overlap >= len(c_words) * 0.5:
            continue
        return _zoho_record_to_entry(record)

    return None


# ---------------------------------------------------------------------------
# Person-level contact check
# ---------------------------------------------------------------------------

def _check_person_in_zoho(person_name, company_name, access_token):
    """Check if a person exists as a Contact in Zoho CRM.

    Returns dict with:
        - found: bool
        - contact_account: str (the Account the contact is currently linked to)
        - contact_account_type: str (klant/prospect/lead)
        - same_company: bool (contact is at the same company as the signal)

    This enables two key business signals:
        1. Person at Account X in Zoho → signal says they're now at Account Y
           → SALES signal (they know IDS, warm intro at new practice)
        2. New person at existing klant Account
           → SUPPORT signal (new decision-maker, update relationship)
    """
    contacts = _search_contact_by_name(access_token, person_name)
    if not contacts:
        return None

    # Find best matching contact
    person_norm = person_name.lower().strip()
    for contact in contacts:
        contact_name = contact.get('Full_Name', '').lower().strip()
        if not contact_name:
            continue

        # Check name overlap
        person_parts = set(person_norm.split())
        contact_parts = set(contact_name.split())
        overlap = person_parts & contact_parts
        if len(overlap) < 1:
            continue

        # Last name must match
        person_last = person_norm.split()[-1] if person_norm.split() else ''
        contact_last = contact_name.split()[-1] if contact_name.split() else ''
        if person_last != contact_last:
            continue

        # Extract account info
        account_info = contact.get('Account_Name')
        if isinstance(account_info, dict):
            contact_account = account_info.get('name', '')
            contact_account_id = account_info.get('id', '')
        elif isinstance(account_info, str):
            contact_account = account_info
            contact_account_id = ''
        else:
            contact_account = ''
            contact_account_id = ''

        # Check if same company
        same_company = False
        if company_name and contact_account:
            comp_norm = _normalize_name(company_name)
            acct_norm = _normalize_name(contact_account)
            if comp_norm and acct_norm:
                same_company = (
                    comp_norm == acct_norm
                    or comp_norm in acct_norm
                    or acct_norm in comp_norm
                )

        return {
            'found': True,
            'contact_name': contact.get('Full_Name', ''),
            'contact_account': contact_account,
            'contact_account_id': contact_account_id,
            'same_company': same_company,
        }

    return None


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def _ensure_columns():
    """Add crm columns to signalen table if they don't exist."""
    conn = get_connection()
    for col in ('crm_status', 'crm_signal_type', 'crm_contact_info'):
        try:
            conn.execute(f"ALTER TABLE signalen ADD COLUMN {col} TEXT")
            conn.commit()
        except Exception:
            pass
    conn.close()


# ---------------------------------------------------------------------------
# Main enrichment function
# ---------------------------------------------------------------------------

def enrich_crm_status(embedded_data_path=None):
    """Check all signals against CRM and update their crm_status.

    In live mode (Zoho credentials present), also performs person-level
    contact checks and sets crm_signal_type:
        - "sales"   → person known in Zoho, moved to new practice
        - "support" → new person at existing klant practice
        - None      → standard account-level match only

    Args:
        embedded_data_path: Path to embedded-data.js. Auto-detected if None.
    """
    _ensure_columns()

    live_mode = _has_zoho_credentials()
    access_token = None

    if live_mode:
        print('[LIVE MODE] Zoho CRM credentials detected — using searchRecords API')
        try:
            access_token = _get_access_token()
            print('  Access token obtained')
        except Exception as e:
            print(f"  WARNING: Failed to get Zoho token ({e}), falling back to offline mode")
            live_mode = False

    if not live_mode:
        print('[OFFLINE MODE] Using embedded-data.js for CRM check')
        crm_accounts = _load_crm_data(embedded_data_path)
        if not crm_accounts:
            print('  CRM check overgeslagen: geen CRM data gevonden.')
            return

        print(f"  CRM data: {len(crm_accounts)} accounts ("
              f"{sum(1 for a in crm_accounts if a['type'] == 'klant')} klanten, "
              f"{sum(1 for a in crm_accounts if a['type'] == 'prospect')} prospects, "
              f"{sum(1 for a in crm_accounts if a['type'] == 'lead')} leads)")

        # Build name index for offline matching
        name_index = {}
        for account in crm_accounts:
            key = _normalize_name(account['name'])
            if not key:
                continue
            name_index[key] = account

    # Get signals without CRM status
    conn = get_connection(readonly=True)
    rows = conn.execute("""
        SELECT id, type, titel, omschrijving FROM signalen
        WHERE crm_status IS NULL OR crm_status = ''
    """).fetchall()
    conn.close()

    if not rows:
        print('  Alle signalen hebben al een CRM status.')
        return

    print(f"  {len(rows)} signalen controleren...")

    # Process each signal
    write_conn = get_connection()
    matched = {'klant': 0, 'prospect': 0, 'lead': 0, 'nieuw': 0}
    signal_types = {'sales': 0, 'support': 0}

    for i, row in enumerate(rows):
        signal_id = row['id']
        signal_type = row['type']
        titel = row['titel'] or ''
        omschrijving = row['omschrijving'] or ''

        # Extract company name from signal
        company = _extract_company_from_signal(titel, omschrijving)
        if not company:
            write_conn.execute(
                "UPDATE signalen SET crm_status = 'nieuw' WHERE id = ?",
                (signal_id,))
            matched['nieuw'] += 1
            continue

        # Find company in CRM
        if live_mode:
            crm_match = _find_in_crm_live(company, access_token)
        else:
            crm_match = _find_in_crm(company, name_index)

        status = crm_match['type'] if crm_match else 'nieuw'
        crm_name = crm_match['name'] if crm_match else ''
        stad = crm_match.get('stad', '') if crm_match else ''

        # Person-level checks for nieuwe_manager signals
        crm_signal_type = None
        crm_contact_info = None

        if signal_type == 'nieuwe_manager':
            person = _extract_person_from_signal(titel, omschrijving)

            if live_mode and person:
                try:
                    contact_result = _check_person_in_zoho(person, company, access_token)

                    if contact_result and contact_result['found']:
                        if not contact_result['same_company'] and contact_result['contact_account']:
                            # Person moved to a new practice — SALES signal
                            crm_signal_type = 'sales'
                            crm_contact_info = json.dumps({
                                'person': person,
                                'previous_account': contact_result['contact_account'],
                                'new_company': company,
                                'reason': 'Bekende contactpersoon verhuisd naar nieuwe praktijk',
                            }, ensure_ascii=False)
                            signal_types['sales'] += 1
                        else:
                            # Person at existing klant — could be SUPPORT
                            if status == 'klant':
                                crm_signal_type = 'support'
                                crm_contact_info = json.dumps({
                                    'person': person,
                                    'account': crm_name,
                                    'reason': 'Nieuwe contactpersoon bij bestaande klant',
                                }, ensure_ascii=False)
                                signal_types['support'] += 1
                            elif status in ('prospect', 'lead'):
                                crm_signal_type = 'sales'
                                crm_contact_info = json.dumps({
                                    'person': person,
                                    'account': crm_name,
                                    'crm_status': status,
                                    'reason': f"Nieuwe beslisser bij bestaande {status}",
                                }, ensure_ascii=False)
                                signal_types['sales'] += 1
                except Exception as e:
                    logger.warning(f"Contact check failed for {person}: {e}")

            elif not live_mode and person and status in ('klant', 'prospect', 'lead'):
                # Offline mode: limited person-level signals
                if status == 'klant':
                    crm_signal_type = 'support'
                    crm_contact_info = json.dumps({
                        'person': person,
                        'account': crm_name,
                        'reason': 'Nieuwe praktijkmanager bij bestaande klant (contactcheck niet beschikbaar)',
                    }, ensure_ascii=False)
                    signal_types['support'] += 1
                else:
                    # prospect or lead
                    crm_signal_type = 'sales'
                    crm_contact_info = json.dumps({
                        'person': person,
                        'account': crm_name,
                        'crm_status': status,
                        'reason': f"Nieuwe beslisser bij bestaande {status} (contactcheck niet beschikbaar)",
                    }, ensure_ascii=False)
                    signal_types['sales'] += 1

        # Update database
        write_conn.execute(
            """UPDATE signalen
               SET crm_status = ?,
                   crm_signal_type = ?,
                   crm_contact_info = ?,
                   gemeente = COALESCE(gemeente, ?)
               WHERE id = ?""",
            (status, crm_signal_type, crm_contact_info, stad, signal_id))

        matched[status] += 1

        # Rate limiting for live mode
        if not live_mode:
            continue
        if (i + 1) % 5 == 0:
            time.sleep(0.3)

    write_conn.commit()
    write_conn.close()

    print('\n  Resultaat:')
    print(f"    Bestaande klanten:   {matched['klant']}")
    print(f"    Prospects:           {matched['prospect']}")
    print(f"    Leads:               {matched['lead']}")
    print(f"    Nieuw (niet in CRM): {matched['nieuw']}")

    if signal_types['sales'] or signal_types['support']:
        print('\n  Persoons-signalen:')
        print(f"    SALES  (persoon verhuisd):  {signal_types['sales']}")
        print(f"    SUPPORT (nieuwe PM klant):  {signal_types['support']}")
        if not live_mode:
            print('    ℹ  Offline mode: SALES signalen vereisen Zoho credentials')
