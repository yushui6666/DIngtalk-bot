#!/bin/bash
DIR="$(cd "$(dirname "$0")/.." && pwd)"
PORT=${1:-8899}
echo "启动管理后台 http://127.0.0.1:$PORT  (DB: $DIR/data/tickets.db)"
echo "密码: ${ADMIN_PASSWORD:-admin123}"
python3 "$DIR/admin/server.py" --port "$PORT"
