#!/bin/bash
# Weekly run script for Huisarts Verbouwing Scraper
# Called by n8n Schedule Trigger or cron
# Usage: ./weekly_run.sh [--full]

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# Activate venv
source .venv/bin/activate

LOG_FILE="data/weekly_$(date +%Y%m%d).log"
mkdir -p data

echo "=== Huisarts Scraper Weekly Run: $(date) ===" | tee -a "$LOG_FILE"

# Step 1: Fetch new bekendmakingen (last 7 days)
echo "Step 1: Ophalen bekendmakingen..." | tee -a "$LOG_FILE"
python main.py --since 7d --enrich 2>&1 | tee -a "$LOG_FILE"

# Step 2: Optionally refresh practices (monthly, pass --full flag)
if [ "$1" = "--full" ]; then
    echo "Step 2: Verversing praktijken..." | tee -a "$LOG_FILE"
    python main.py --refresh-practices --delay 3 2>&1 | tee -a "$LOG_FILE"
fi

# Step 3: Export results
echo "Step 3: Exporteren..." | tee -a "$LOG_FILE"
python -c "
import csv, json, sqlite3
from pathlib import Path

DB = Path('data/huisarts.db')
conn = sqlite3.connect(f'file:{DB}?mode=ro', uri=True)
conn.row_factory = sqlite3.Row

# Matches export
rows = conn.execute('''
    SELECT p.naam, p.agb_code, p.adres, p.postcode, p.stad, p.telefoon, p.website,
           s.type, s.titel, s.bron_url, s.publicatiedatum, s.gemeente,
           m.match_score, m.match_type
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

# Print stats
echo "" | tee -a "$LOG_FILE"
python main.py --stats 2>&1 | tee -a "$LOG_FILE"

echo "" | tee -a "$LOG_FILE"
echo "=== Klaar: $(date) ===" | tee -a "$LOG_FILE"
