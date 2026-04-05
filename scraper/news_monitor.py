"""Monitor Dutch healthcare news sites for practice change signals.

Scrapes RSS feeds and sitemaps of key Dutch healthcare publications
to detect 4 types of sales triggers:
1. Verbouwing/verhuizing — renovation or relocation
2. Fusie — practice mergers
3. Nieuwe praktijkmanager — new practice manager
4. Zorggroep aansluiting — joining a care group or chain
"""

import re
import time
import logging
from datetime import datetime, timedelta
from xml.etree import ElementTree as ET

import requests
from bs4 import BeautifulSoup
from scraper.db import insert_signaal

logger = logging.getLogger(__name__)

HEADERS = {
    'User-Agent': 'HuisartsScraper/2.0 (lead-generation tool)',
    'Accept': 'text/html,application/xml,application/rss+xml',
}

RSS_FEEDS = [
    'https://www.skipr.nl/rss/',
    'https://www.medischcontact.nl/rss',
    'https://www.zorgvisie.nl/rss/',
    'https://www.lhv.nl/rss.xml',
    'https://www.de-eerstelijns.nl/feed/',
]

GP_CONTEXT_KEYWORDS = [
    'huisarts', 'huisartsenpraktijk', 'huisartspraktijk',
    'gezondheidscentrum', 'eerstelijn', 'praktijk',
    'medisch centrum', 'huisartsenpost',
]

TRIGGER_PATTERNS = {
    'verbouwing': {
        'keywords': [
            'verbouw', 'nieuwbouw', 'renovatie', 'verhuiz',
            'nieuwe locatie', 'nieuwe huisvesting', 'bouwplan',
            'oplevering', 'herontwikkeling', 'sloop',
            'omgevingsvergunning', 'bouwvergunning',
        ],
        'min_relevance': 1,
    },
    'fusie': {
        'keywords': [
            'fusie', 'fuseren', 'gefuseerd', 'samengaan',
            'samenvoeging', 'samengevoegd', 'samen verder',
            'opgegaan in', 'samenwerking', 'bundelen',
        ],
        'min_relevance': 1,
    },
    'nieuwe_manager': {
        'keywords': [
            'praktijkmanager', 'nieuwe manager', 'aangesteld',
            'benoemd', 'praktijkhouder', 'overdracht', 'opvolg',
            'eigenaar', 'waarneming',
        ],
        'min_relevance': 1,
    },
    'zorggroep': {
        'keywords': [
            'zorggroep', 'aangesloten', 'overname', 'overgenomen',
            'keten', 'co\u00f6peratie', 'toetreding', 'concern',
            'onderdeel van', 'co-med',
        ],
        'min_relevance': 1,
    },
}


def monitor_news(since: str = None, delay: float = 1.5) -> int:
    """Monitor Dutch healthcare news feeds for practice change signals.

Args:
    since: Date string YYYY-MM-DD. Only process articles newer than this.
    delay: Seconds between HTTP requests.

Returns:
    Number of new signals stored.
"""
    if not since:
        since = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')

    since_dt = datetime.strptime(since, '%Y-%m-%d')

    print(f'Nieuwsmonitoring starten (sinds {since})...')
    session = requests.Session()
    session.headers.update(HEADERS)
    total_new = 0

    # Process RSS feeds
    for feed_url in RSS_FEEDS:
        source = _extract_domain(feed_url)
        print(f'  RSS: {source}')
        try:
            new = _process_rss_feed(session, feed_url, since_dt, delay)
            total_new += new
            if new > 0:
                print(f'    \u2192 {new} nieuwe signalen')
        except Exception as e:
            logger.warning(f'RSS fout ({source}): {e}')
            print(f'    Fout: {e}')

    # Scrape article listing pages
    print("\n  Artikelpagina's doorzoeken...")
    article_sources = [
        ('https://www.skipr.nl/nieuws/', 'skipr.nl'),
        ('https://www.lhv.nl/actueel/nieuws/', 'lhv.nl'),
        ('https://www.de-eerstelijns.nl/category/nieuws/', 'de-eerstelijns.nl'),
    ]

    for url, name in article_sources:
        print(f'  Web: {name}')
        try:
            new = _scrape_article_list(session, url, since_dt, delay)
            total_new += new
            if new > 0:
                print(f'    \u2192 {new} nieuwe signalen')
        except Exception as e:
            logger.warning(f'Scrape fout ({name}): {e}')
            print(f'    Fout: {e}')

    print(f'\nNieuwsmonitoring klaar: {total_new} nieuwe signalen opgeslagen.')
    return total_new


def _process_rss_feed(session: requests.Session, feed_url: str,
                      since_dt: datetime, delay: float) -> int:
    """Parse an RSS feed and store relevant articles as signals."""
    try:
        resp = session.get(feed_url, timeout=15)
        if resp.status_code != 200:
            return 0
    except requests.RequestException as e:
        logger.debug(f'Feed ophalen mislukt {feed_url}: {e}')
        return 0

    new_count = 0

    try:
        root = ET.fromstring(resp.content)
    except ET.ParseError:
        return 0

    # Try RSS <item> elements first, then Atom <entry> elements
    items = root.findall('.//item')
    if not items:
        atom_ns = {'atom': 'http://www.w3.org/2005/Atom'}
        items = root.findall('.//atom:entry', atom_ns)

    for item in items:
        article = _parse_feed_item(item, feed_url)
        if not article:
            continue

        # Filter by date
        if article.get('date'):
            try:
                art_date = datetime.strptime(article['date'], '%Y-%m-%d')
                if art_date < since_dt:
                    continue
            except ValueError:
                pass

        # Classify the article
        trigger_type = _classify_article(article['title'], article.get('description', ''))
        if not trigger_type:
            continue

        result = insert_signaal(
            type=trigger_type,
            titel=article['title'],
            omschrijving=article.get('description'),
            bron_url=article['url'],
            publicatiedatum=article.get('date'),
        )
        if result is None:
            continue
        new_count += 1

    return new_count


def _parse_feed_item(item, feed_url: str) -> dict | None:
    """Parse a single RSS/Atom feed item into a dict."""
    result = {}

    # Try RSS format first
    title = item.findtext('title')
    link = item.findtext('link')
    desc = item.findtext('description')
    pub_date = item.findtext('pubDate')

    # If no title found, try Atom format
    if not title:
        atom_ns = {'atom': 'http://www.w3.org/2005/Atom'}
        title = item.findtext('atom:title', namespaces=atom_ns)
        link_el = item.find("atom:link[@rel='alternate']", atom_ns) or item.find('atom:link', atom_ns)
        link = link_el.get('href') if link_el is not None else None
        desc = item.findtext('atom:summary', namespaces=atom_ns) or item.findtext('atom:content', namespaces=atom_ns)
        pub_date = item.findtext('atom:published', namespaces=atom_ns) or item.findtext('atom:updated', namespaces=atom_ns)

    if not title or not link:
        return None

    result['title'] = _clean_html(title).strip()
    result['url'] = link.strip()
    result['description'] = _clean_html(desc).strip() if desc else ''

    if pub_date:
        result['date'] = _parse_rss_date(pub_date)

    return result


def _scrape_article_list(session: requests.Session, url: str,
                         since_dt: datetime, delay: float) -> int:
    """Scrape an article listing page for relevant articles."""
    try:
        resp = session.get(url, timeout=15)
        if resp.status_code != 200:
            return 0
    except requests.RequestException:
        return 0

    soup = BeautifulSoup(resp.text, 'lxml')
    new_count = 0

    # Find all article links
    article_links = set()
    for a in soup.find_all('a', href=True):
        href = a['href']
        text = a.get_text(strip=True)

        # Skip short or empty link text
        if not text or len(text) < 20:
            continue

        # Normalize URL
        if not href.startswith('http'):
            if href.startswith('/'):
                from urllib.parse import urljoin
                href = urljoin(url, href)
            else:
                continue

        # Classify by link text
        trigger_type = _classify_article(text, '')
        if not trigger_type:
            continue
        article_links.add((href, text, trigger_type))

    # Process up to 15 article links
    for article_url, title, trigger_type in list(article_links)[:15]:
        result = insert_signaal(
            type=trigger_type,
            titel=title,
            bron_url=article_url,
        )
        if result is None:
            continue
        new_count += 1

    return new_count


def _classify_article(title: str, description: str) -> str | None:
    """Classify an article into a trigger type, or None if not relevant.

An article must contain:
1. At least one GP/practice context keyword
2. At least one trigger keyword from any category
"""
    text = f'{title} {description}'.lower()

    # Check for GP/practice context
    has_gp_context = any(kw in text for kw in GP_CONTEXT_KEYWORDS)
    if not has_gp_context:
        return None

    # Score each trigger type
    scores = {}
    for trigger_type, config in TRIGGER_PATTERNS.items():
        score = sum(1 for kw in config['keywords'] if kw in text)
        if score >= config['min_relevance']:
            scores[trigger_type] = score

    if not scores:
        return None

    # Return the trigger type with the highest score
    return max(scores, key=scores.get)


def _clean_html(text: str) -> str:
    if not text:
        return ''
    clean = re.sub('<[^>]+>', ' ', text)
    clean = re.sub(r'\s+', ' ', clean)
    return clean.strip()


def _parse_rss_date(date_str: str) -> str | None:
    if not date_str:
        return None

    # Try ISO format first: 2024-01-15
    m = re.match(r'(\d{4}-\d{2}-\d{2})', date_str)
    if m:
        return m.group(1)

    # Try RFC 822 format: 15 Jan 2024
    months = {
        'jan': '01', 'feb': '02', 'mar': '03', 'apr': '04',
        'may': '05', 'jun': '06', 'jul': '07', 'aug': '08',
        'sep': '09', 'oct': '10', 'nov': '11', 'dec': '12',
    }
    m = re.search(r'(\d{1,2})\s+(\w{3})\s+(\d{4})', date_str)
    if m:
        day = m.group(1).zfill(2)
        month = months.get(m.group(2).lower())
        year = m.group(3)
        if month:
            return f'{year}-{month}-{day}'

    return None


def _extract_domain(url: str) -> str:
    from urllib.parse import urlparse
    parsed = urlparse(url)
    return parsed.netloc.replace('www.', '')
