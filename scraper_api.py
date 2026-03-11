"""Lightweight HTTP API for triggering scraper runs from n8n."""

import json
import os
import subprocess
import sys
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from datetime import datetime

API_TOKEN = os.environ.get("API_TOKEN", "geheim123")
PORT = int(os.environ.get("API_PORT", "8321"))

# Track running jobs
current_job = {"running": False, "started": None, "result": None}


class ScraperHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/health":
            self._respond(200, {"status": "ok", "running": current_job["running"]})
            return

        if self.path == "/status":
            if not self._check_auth():
                return
            self._respond(200, {
                "running": current_job["running"],
                "started": current_job["started"],
                "last_result": current_job["result"],
            })
            return

        self._respond(404, {"error": "not found"})

    def do_POST(self):
        if self.path == "/run":
            if not self._check_auth():
                return

            if current_job["running"]:
                self._respond(409, {"error": "Job already running", "started": current_job["started"]})
                return

            # Start scraper in background thread
            current_job["running"] = True
            current_job["started"] = datetime.now().isoformat()
            current_job["result"] = None

            thread = threading.Thread(target=_run_scraper, daemon=True)
            thread.start()

            self._respond(202, {"status": "started", "started": current_job["started"]})
            return

        self._respond(404, {"error": "not found"})

    def _check_auth(self) -> bool:
        auth = self.headers.get("Authorization", "")
        if auth != f"Bearer {API_TOKEN}":
            self._respond(401, {"error": "unauthorized"})
            return False
        return True

    def _respond(self, code: int, data: dict):
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(data, ensure_ascii=False).encode())

    def log_message(self, format, *args):
        # Quiet logging
        pass


def _run_scraper():
    """Run the daily scraper script and capture output."""
    try:
        result = subprocess.run(
            ["./daily_run_docker.sh"],
            capture_output=True,
            text=True,
            timeout=600,
            cwd="/app",
        )

        # Parse JSON summary from last line
        summary = None
        for line in reversed(result.stdout.split("\n")):
            if line.strip().startswith("JSON_SUMMARY:"):
                try:
                    summary = json.loads(line.strip().replace("JSON_SUMMARY:", ""))
                except json.JSONDecodeError:
                    pass
                break

        current_job["result"] = {
            "exit_code": result.returncode,
            "summary": summary,
            "stdout_tail": "\n".join(result.stdout.split("\n")[-20:]),
            "stderr": result.stderr[:1000] if result.stderr else None,
            "finished": datetime.now().isoformat(),
        }
    except subprocess.TimeoutExpired:
        current_job["result"] = {
            "exit_code": -1,
            "summary": None,
            "error": "Timeout na 10 minuten",
            "finished": datetime.now().isoformat(),
        }
    except Exception as e:
        current_job["result"] = {
            "exit_code": -1,
            "summary": None,
            "error": str(e),
            "finished": datetime.now().isoformat(),
        }
    finally:
        current_job["running"] = False


if __name__ == "__main__":
    print(f"Scraper API starting on port {PORT}...")
    server = HTTPServer(("0.0.0.0", PORT), ScraperHandler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("Shutting down...")
        server.server_close()
