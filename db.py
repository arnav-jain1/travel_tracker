"""
Travel Tracker – share-link backend
====================================
Stores map state objects keyed by a short SHA-256 hash, persisted in SQLite.

Install:  pip install flask flask-cors
Run:      python backend.py
          (use gunicorn/uwsgi in front of this for real production)
"""

import hashlib
import json
import logging
import re
import sqlite3
from pathlib import Path

from flask import Flask, request, jsonify, g
from flask_cors import CORS

# ── Config ─────────────────────────────────────────────────────────────────

MAX_BODY_BYTES   = 5 * 1025 * 1024   # 5 MB – hard ceiling on incoming payload
HASH_LENGTH      = 20           # hex chars kept from SHA-256
ALLOWED_ORIGINS  = [            # tighten this to your actual domain(s)
    "http://localhost",
    "http://127.0.0.1",
    # "https://yourapp.example.com",
]
DATA_DIR = Path(__file__).parent / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

DB_PATH = DATA_DIR / "database.db"

# ── App setup ──────────────────────────────────────────────────────────────

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = MAX_BODY_BYTES  # Flask rejects larger bodies with 413

CORS(app, origins=ALLOWED_ORIGINS)


# ── Database helpers ───────────────────────────────────────────────────────

def get_db() -> sqlite3.Connection:
    if "db" not in g:
        g.db = sqlite3.connect(DB_PATH)
        g.db.row_factory = sqlite3.Row
        g.db.execute("PRAGMA journal_mode=WAL")   # safe for concurrent reads
        g.db.execute("PRAGMA foreign_keys=ON")
    return g.db


@app.teardown_appcontext
def close_db(exc=None):
    db = g.pop("db", None)
    if db is not None:
        db.close()


def init_db():
    with sqlite3.connect(DB_PATH) as con:
        con.execute("""
            CREATE TABLE IF NOT EXISTS shares (
                hash      TEXT PRIMARY KEY CHECK(length(hash) = 20),
                state     TEXT NOT NULL,
                created   DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        con.commit()
    log.info("Database ready at %s", DB_PATH)


# ── Input helpers ──────────────────────────────────────────────────────────

_HASH_RE = re.compile(r"^[0-9a-f]{20}$")

def is_valid_hash(val: str) -> bool:
    return bool(_HASH_RE.match(val))


def validate_state(state) -> str | None:
    """
    Return an error string if state is unacceptable, else None.
    Checks type and that all keys/values are plain JSON-safe data
    (no injected objects with __proto__ etc.).
    """
    if not isinstance(state, dict):
        return "state must be a JSON object"
    # Re-serialise and check the byte length independently of the HTTP body limit
    try:
        canonical = json.dumps(state, sort_keys=True, separators=(",", ":"))
    except (TypeError, ValueError):
        return "state is not serialisable"
    if len(canonical.encode()) > MAX_BODY_BYTES:
        return "state payload too large"
    return None


# ── Routes ─────────────────────────────────────────────────────────────────

@app.route("/save", methods=["POST"])
def save():
    if not request.is_json:
        return jsonify({"error": "Content-Type must be application/json"}), 415

    body = request.get_json(silent=True)
    if body is None:
        return jsonify({"error": "Invalid JSON body"}), 400

    state = body.get("state")
    err = validate_state(state)
    if err:
        return jsonify({"error": err}), 400

    canonical = json.dumps(state, sort_keys=True, separators=(",", ":"))
    short_hash = hashlib.sha256(canonical.encode()).hexdigest()[:HASH_LENGTH]

    db = get_db()
    db.execute(
        "INSERT OR REPLACE INTO shares (hash, state) VALUES (?, ?)",
        (short_hash, canonical),
    )
    db.commit()
    log.info("Saved share %s (%d bytes)", short_hash, len(canonical))

    return jsonify({"hash": short_hash}), 201


@app.route("/load/<hash_val>", methods=["GET"])
def load(hash_val: str):
    if not is_valid_hash(hash_val):
        return jsonify({"error": "Invalid hash format"}), 400

    row = get_db().execute(
        "SELECT state FROM shares WHERE hash = ?", (hash_val,)
    ).fetchone()

    if row is None:
        return jsonify({"error": "Not found"}), 404

    return jsonify({"state": json.loads(row["state"])})


# ── Generic error handlers ─────────────────────────────────────────────────

@app.errorhandler(413)
def too_large(_):
    return jsonify({"error": f"Payload exceeds {MAX_BODY_BYTES // 1024 // 1024} MB limit"}), 413

@app.errorhandler(405)
def method_not_allowed(_):
    return jsonify({"error": "Method not allowed"}), 405

@app.errorhandler(500)
def internal(_):
    return jsonify({"error": "Internal server error"}), 500


# ── Entry point ────────────────────────────────────────────────────────────

if __name__ == "__main__":
    init_db()
    # For production, run behind gunicorn:
    #   gunicorn -w 4 -b 0.0.0.0:5000 backend:app
    app.run(host="127.0.0.1", port=5000, debug=False)
