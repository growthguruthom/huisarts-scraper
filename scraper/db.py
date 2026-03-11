"""SQLite database layer for huisarts-scraper."""

import sqlite3
from pathlib import Path
from datetime import datetime

DB_PATH = Path(__file__).parent.parent / "data" / "huisarts.db"


def get_connection(readonly: bool = False) -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    if readonly:
        uri = f"file:{DB_PATH}?mode=ro"
        conn = sqlite3.connect(uri, uri=True, timeout=10)
        conn.row_factory = sqlite3.Row
    else:
        conn = sqlite3.connect(str(DB_PATH), timeout=10)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db():
    """Create tables if they don't exist."""
    conn = get_connection()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS praktijken (
            agb_code TEXT PRIMARY KEY,
            naam TEXT NOT NULL,
            adres TEXT,
            postcode TEXT,
            stad TEXT,
            telefoon TEXT,
            website TEXT,
            lat REAL,
            lon REAL,
            bron TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS signalen (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            type TEXT NOT NULL,
            titel TEXT,
            omschrijving TEXT,
            adres TEXT,
            postcode TEXT,
            stad TEXT,
            gemeente TEXT,
            bron_url TEXT UNIQUE,
            publicatiedatum DATE,
            gevonden_op TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS matches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            praktijk_agb TEXT REFERENCES praktijken(agb_code),
            signaal_id INTEGER REFERENCES signalen(id),
            match_score TEXT,
            match_type TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(praktijk_agb, signaal_id)
        );

        CREATE INDEX IF NOT EXISTS idx_praktijken_postcode ON praktijken(postcode);
        CREATE INDEX IF NOT EXISTS idx_praktijken_stad ON praktijken(stad);
        CREATE INDEX IF NOT EXISTS idx_signalen_type ON signalen(type);
        CREATE INDEX IF NOT EXISTS idx_signalen_datum ON signalen(publicatiedatum);
    """)
    conn.close()


def upsert_praktijk(agb_code: str, naam: str, adres: str = None,
                    postcode: str = None, stad: str = None,
                    telefoon: str = None, website: str = None,
                    lat: float = None, lon: float = None, bron: str = "vektis"):
    conn = get_connection()
    conn.execute("""
        INSERT INTO praktijken (agb_code, naam, adres, postcode, stad, telefoon, website, lat, lon, bron, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(agb_code) DO UPDATE SET
            naam=excluded.naam, adres=excluded.adres, postcode=excluded.postcode,
            stad=excluded.stad, telefoon=excluded.telefoon, website=excluded.website,
            lat=excluded.lat, lon=excluded.lon, bron=excluded.bron,
            updated_at=excluded.updated_at
    """, (agb_code, naam, adres, postcode, stad, telefoon, website, lat, lon, bron, datetime.now().isoformat()))
    conn.commit()
    conn.close()


def insert_signaal(type: str, titel: str, omschrijving: str = None,
                   adres: str = None, postcode: str = None, stad: str = None,
                   gemeente: str = None, bron_url: str = None,
                   publicatiedatum: str = None) -> int | None:
    """Insert a signaal, returns id. Returns None if bron_url already exists."""
    conn = get_connection()
    try:
        cursor = conn.execute("""
            INSERT INTO signalen (type, titel, omschrijving, adres, postcode, stad, gemeente, bron_url, publicatiedatum)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (type, titel, omschrijving, adres, postcode, stad, gemeente, bron_url, publicatiedatum))
        conn.commit()
        return cursor.lastrowid
    except sqlite3.IntegrityError:
        return None
    finally:
        conn.close()


def insert_match(praktijk_agb: str, signaal_id: int, match_score: str, match_type: str):
    conn = get_connection()
    try:
        conn.execute("""
            INSERT OR IGNORE INTO matches (praktijk_agb, signaal_id, match_score, match_type)
            VALUES (?, ?, ?, ?)
        """, (praktijk_agb, signaal_id, match_score, match_type))
        conn.commit()
    finally:
        conn.close()


def get_praktijken() -> list[dict]:
    conn = get_connection(readonly=True)
    rows = conn.execute("SELECT * FROM praktijken ORDER BY stad, naam").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_signalen(since: str = None) -> list[dict]:
    conn = get_connection(readonly=True)
    if since:
        rows = conn.execute(
            "SELECT * FROM signalen WHERE gevonden_op >= ? ORDER BY publicatiedatum DESC", (since,)
        ).fetchall()
    else:
        rows = conn.execute("SELECT * FROM signalen ORDER BY publicatiedatum DESC").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_dashboard_data() -> list[dict]:
    """Get joined matches + praktijken + signalen for the dashboard."""
    conn = get_connection(readonly=True)
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


def get_unmatched_signalen() -> list[dict]:
    """Get signalen that have no match with any praktijk."""
    conn = get_connection(readonly=True)
    rows = conn.execute("""
        SELECT s.* FROM signalen s
        LEFT JOIN matches m ON s.id = m.signaal_id
        WHERE m.id IS NULL
        ORDER BY s.publicatiedatum DESC
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_stats() -> dict:
    conn = get_connection(readonly=True)
    stats = {
        "praktijken": conn.execute("SELECT COUNT(*) FROM praktijken").fetchone()[0],
        "signalen": conn.execute("SELECT COUNT(*) FROM signalen").fetchone()[0],
        "matches": conn.execute("SELECT COUNT(*) FROM matches").fetchone()[0],
        "gemeenten": conn.execute("SELECT COUNT(DISTINCT gemeente) FROM signalen WHERE gemeente IS NOT NULL").fetchone()[0],
        "laatste_update": conn.execute("SELECT MAX(gevonden_op) FROM signalen").fetchone()[0],
    }
    conn.close()
    return stats
