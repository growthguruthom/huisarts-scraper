#!/bin/bash
# Start the lightweight dashboard server
# Usage: ./start_dashboard.sh [port]

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

PORT="${1:-8502}"

# Kill existing dashboard
pkill -f "dashboard/server.py" 2>/dev/null
sleep 1

# Start dashboard
source .venv/bin/activate
nohup python dashboard/server.py "$PORT" > /tmp/dashboard.log 2>&1 &
echo "Dashboard gestart op http://localhost:$PORT (PID: $!)"
