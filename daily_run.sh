#!/bin/bash
# Daily run script for Huisarts Verbouwing Scraper
# Called by n8n Schedule Trigger (daily at 08:00)
# Outputs JSON summary on last line for n8n parsing

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# Activate venv
source .venv/bin/activate

LOG_FILE="data/daily_$(date +%Y%m%d).log"
mkdir -p data

echo "=== Huisarts Scraper Daily Run: $(date) ===" | tee -a "$LOG_FILE"

# Count matches before run
MATCHES_BEFORE=$(python -c "
import sqlite3
from pathlib import Path
db = Path('data/huisarts.db')
if db.exists():
    conn = sqlite3.connect(f'file:{db}?mode=ro', uri=True)
    print(conn.execute('SELECT COUNT(*) FROM matches').fetchone()[0])
    conn.close()
else:
    print(0)
" 2>/dev/null || echo 0)

SIGNALEN_BEFORE=$(python -c "
import sqlite3
from pathlib import Path
db = Path('data/huisarts.db')
if db.exists():
    conn = sqlite3.connect(f'file:{db}?mode=ro', uri=True)
    print(conn.execute('SELECT COUNT(*) FROM signalen').fetchone()[0])
    conn.close()
else:
    print(0)
" 2>/dev/null || echo 0)

# Step 1: Fetch new bekendmakingen (last 2 days for overlap safety)
echo "Step 1: Ophalen bekendmakingen (laatste 2 dagen)..." | tee -a "$LOG_FILE"
python main.py --since 2d --enrich --delay 1.5 2>&1 | tee -a "$LOG_FILE"

# Step 2: Export matches to CSV
echo "Step 2: Exporteren..." | tee -a "$LOG_FILE"
python -c "
import csv, sqlite3
from pathlib import Path

DB = Path('data/huisarts.db')
conn = sqlite3.connect(f'file:{DB}?mode=ro', uri=True)
conn.row_factory = sqlite3.Row

rows = conn.execute('''
    SELECT p.naam, p.agb_code, p.adres, p.postcode, p.stad, p.telefoon, p.website,
           s.type, s.titel, s.bron_url, s.publicatiedatum, s.gemeente,
           m.match_score, m.match_type, m.created_at
    FROM matches m
    JOIN praktijken p ON m.praktijk_agb = p.agb_code
    JOIN signalen s ON m.signaal_id = s.id
    ORDER BY m.created_at DESC
''').fetchall()

with open('data/matches_export.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow([d[0] for d in rows[0].keys()] if rows else [])
    for r in rows:
        writer.writerow(list(r))

conn.close()
print(f'Exported {len(rows)} matches to data/matches_export.csv')
" 2>&1 | tee -a "$LOG_FILE"

# Count matches after run
MATCHES_AFTER=$(python -c "
import sqlite3
conn = sqlite3.connect('file:data/huisarts.db?mode=ro', uri=True)
print(conn.execute('SELECT COUNT(*) FROM matches').fetchone()[0])
conn.close()
")

SIGNALEN_AFTER=$(python -c "
import sqlite3
conn = sqlite3.connect('file:data/huisarts.db?mode=ro', uri=True)
print(conn.execute('SELECT COUNT(*) FROM signalen').fetchone()[0])
conn.close()
")

NEW_MATCHES=$((MATCHES_AFTER - MATCHES_BEFORE))
NEW_SIGNALEN=$((SIGNALEN_AFTER - SIGNALEN_BEFORE))

# Get today's new matches details as JSON
TODAY_MATCHES=$(python -c "
import json, sqlite3
from datetime import datetime, timedelta

conn = sqlite3.connect('file:data/huisarts.db?mode=ro', uri=True)
conn.row_factory = sqlite3.Row
since = (datetime.now() - timedelta(hours=24)).isoformat()
rows = conn.execute('''
    SELECT p.naam, p.stad, s.titel, s.gemeente, m.match_type, m.match_score
    FROM matches m
    JOIN praktijken p ON m.praktijk_agb = p.agb_code
    JOIN signalen s ON m.signaal_id = s.id
    WHERE m.created_at >= ?
    ORDER BY m.created_at DESC
    LIMIT 20
''', (since,)).fetchall()
conn.close()
print(json.dumps([dict(r) for r in rows], ensure_ascii=False))
")

# Get total stats
TOTAL_PRAKTIJKEN=$(python -c "
import sqlite3
conn = sqlite3.connect('file:data/huisarts.db?mode=ro', uri=True)
print(conn.execute('SELECT COUNT(*) FROM praktijken').fetchone()[0])
conn.close()
")

TOTAL_GEMEENTEN=$(python -c "
import sqlite3
conn = sqlite3.connect('file:data/huisarts.db?mode=ro', uri=True)
print(conn.execute('SELECT COUNT(DISTINCT gemeente) FROM signalen WHERE gemeente IS NOT NULL').fetchone()[0])
conn.close()
")

echo "" | tee -a "$LOG_FILE"
echo "=== Klaar: $(date) ===" | tee -a "$LOG_FILE"

# LAST LINE: JSON summary for n8n to parse
echo "JSON_SUMMARY:{\"status\":\"success\",\"date\":\"$(date +%Y-%m-%d)\",\"new_signalen\":${NEW_SIGNALEN},\"new_matches\":${NEW_MATCHES},\"total_signalen\":${SIGNALEN_AFTER},\"total_matches\":${MATCHES_AFTER},\"total_praktijken\":${TOTAL_PRAKTIJKEN},\"total_gemeenten\":${TOTAL_GEMEENTEN},\"today_matches\":${TODAY_MATCHES}}"
