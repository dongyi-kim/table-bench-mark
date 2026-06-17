#!/usr/bin/env bash
# Re-run the compaction combos that hit transient Polaris errors, appending to the
# existing results dir, then regenerate the report.
set -uo pipefail
cd "$(dirname "$0")/.."
COMPOSE="docker-compose"
RESDIR="/results/bench-20260616-133514"

COMBOS=(
  "iceberg-v2-cow every_round"
  "iceberg-v2-mor every_10_rounds"
  "iceberg-v2-mor every_round"
  "iceberg-v3-cow every_10_rounds"
  "iceberg-v3-cow every_round"
  "iceberg-v3-mor every_round"
)

wait_spark() {  # timeout 240s, retry recreate up to 3x
  local a
  for a in 1 2 3; do
    $COMPOSE up -d --force-recreate --no-deps spark >/dev/null 2>&1
    local w=0
    while [ "$w" -lt 240 ]; do
      [ "$(docker inspect -f '{{.State.Health.Status}}' "$($COMPOSE ps -q spark)" 2>/dev/null)" = "healthy" ] && { echo "  spark healthy"; return 0; }
      sleep 5; w=$((w+5))
    done
    echo "  spark 미healthy, 재생성 $a"
  done
  return 0
}

for combo in "${COMBOS[@]}"; do
  set -- $combo; cand="$1"; comp="$2"
  echo "============================================================"
  echo "[rerun] $cand / $comp"
  wait_spark
  $COMPOSE exec -T bench-runner python -m bench bench \
    --candidate "$cand" --compaction "$comp" \
    --query-engine spark --results-dir "$RESDIR" --skip-gen --no-report
done

echo "[rerun] 리포트 재생성..."
$COMPOSE exec -T bench-runner python -m bench report --results-dir "$RESDIR"
echo "[rerun] 완료: ${RESDIR}/report.md"
