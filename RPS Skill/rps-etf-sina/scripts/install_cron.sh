#!/usr/bin/env bash
set -euo pipefail

SKILL_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_BIN="${PYTHON_BIN:-python3}"
DB_PATH="${DB_PATH:-$SKILL_DIR/data/etf_daily.duckdb}"
OUT_PATH="${OUT_PATH:-$SKILL_DIR/output/rps_etf_top100.png}"
LOG_PATH="${LOG_PATH:-$SKILL_DIR/logs/rps_etf_sina.log}"

mkdir -p "$(dirname "$DB_PATH")" "$(dirname "$OUT_PATH")" "$(dirname "$LOG_PATH")"

BEGIN_MARK="# BEGIN rps-etf-sina"
END_MARK="# END rps-etf-sina"
TMP_CRON="$(mktemp)"

existing_cron="$(crontab -l 2>/dev/null || true)"
printf "%s\n" "$existing_cron" | awk -v begin="$BEGIN_MARK" -v end="$END_MARK" '
  $0 == begin { skip = 1; next }
  $0 == end { skip = 0; next }
  skip != 1 { print }
' > "$TMP_CRON"

{
  echo "$BEGIN_MARK"
  echo "CRON_TZ=Asia/Shanghai"
  echo "0 9 * * * export TZ=Asia/Shanghai; cd \"$SKILL_DIR\" && \"$PYTHON_BIN\" scripts/rps_etf_sina.py run --db \"$DB_PATH\" --out \"$OUT_PATH\" >> \"$LOG_PATH\" 2>&1"
  echo "$END_MARK"
} >> "$TMP_CRON"

crontab "$TMP_CRON"
rm -f "$TMP_CRON"

echo "Installed rps-etf-sina cron job."
echo "Daily run: 09:00 Asia/Shanghai"
echo "Database: $DB_PATH"
echo "Image: $OUT_PATH"
echo "Log: $LOG_PATH"
