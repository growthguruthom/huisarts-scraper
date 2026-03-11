"""Lightweight dashboard server - no heavy dependencies (pandas/streamlit).

Uses only built-in Python modules + sqlite3.
Serves a single HTML page with embedded JS for interactivity.
"""

import json
import sqlite3
import sys
from http.server import HTTPServer, SimpleHTTPRequestHandler
from pathlib import Path
from urllib.parse import urlparse, parse_qs

DB_PATH = Path(__file__).parent.parent / "data" / "huisarts.db"
DASHBOARD_DIR = Path(__file__).parent


def get_stats():
    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True, timeout=5)
    conn.row_factory = sqlite3.Row
    stats = {
        "praktijken": conn.execute("SELECT COUNT(*) FROM praktijken").fetchone()[0],
        "signalen": conn.execute("SELECT COUNT(*) FROM signalen").fetchone()[0],
        "matches": conn.execute("SELECT COUNT(*) FROM matches").fetchone()[0],
        "gemeenten": conn.execute("SELECT COUNT(DISTINCT gemeente) FROM signalen WHERE gemeente IS NOT NULL").fetchone()[0],
        "laatste_update": conn.execute("SELECT MAX(gevonden_op) FROM signalen").fetchone()[0],
    }
    conn.close()
    return stats


def get_matches():
    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True, timeout=5)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("""
        SELECT
            p.naam AS praktijk_naam,
            p.agb_code,
            p.adres AS praktijk_adres,
            p.postcode AS praktijk_postcode,
            p.stad AS praktijk_stad,
            p.telefoon,
            p.website,
            p.lat,
            p.lon,
            s.type AS signaal_type,
            s.titel AS signaal_titel,
            s.omschrijving,
            s.bron_url,
            s.publicatiedatum,
            s.gemeente,
            m.match_score,
            m.match_type,
            m.created_at
        FROM matches m
        JOIN praktijken p ON m.praktijk_agb = p.agb_code
        JOIN signalen s ON m.signaal_id = s.id
        ORDER BY m.created_at DESC
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_unmatched():
    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True, timeout=5)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("""
        SELECT s.* FROM signalen s
        LEFT JOIN matches m ON s.id = m.signaal_id
        WHERE m.id IS NULL
        ORDER BY s.publicatiedatum DESC
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]


class DashboardHandler(SimpleHTTPRequestHandler):
    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path

        if path == "/" or path == "/index.html":
            self._serve_file(DASHBOARD_DIR / "index.html", "text/html")
        elif path == "/api/stats":
            self._json_response(get_stats())
        elif path == "/api/matches":
            self._json_response(get_matches())
        elif path == "/api/unmatched":
            self._json_response(get_unmatched())
        else:
            self.send_error(404)

    def _json_response(self, data):
        body = json.dumps(data, default=str, ensure_ascii=False).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", len(body))
        self.end_headers()
        self.wfile.write(body)

    def _serve_file(self, filepath, content_type):
        try:
            content = filepath.read_bytes()
            self.send_response(200)
            self.send_header("Content-Type", f"{content_type}; charset=utf-8")
            self.send_header("Content-Length", len(content))
            self.end_headers()
            self.wfile.write(content)
        except FileNotFoundError:
            self.send_error(404)

    def log_message(self, format, *args):
        pass  # Suppress access logs


def main():
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8502
    server = HTTPServer(("0.0.0.0", port), DashboardHandler)
    print(f"Dashboard: http://localhost:{port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nGestopt.")


if __name__ == "__main__":
    main()
