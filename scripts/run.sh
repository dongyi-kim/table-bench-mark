#!/usr/bin/env bash
# table-bench-mark orchestration wrapper around docker compose + the bench runner.
set -euo pipefail

cd "$(dirname "$0")/.."

# Prefer standalone docker-compose (v1.29+) — it reads .env for build-args and supports
# depends_on healthcheck conditions; fall back to the integrated `docker compose`.
if command -v docker-compose >/dev/null 2>&1; then
  COMPOSE="docker-compose"
else
  COMPOSE="docker compose"
fi
RUNNER="bench-runner"

ensure_env() {
  if [ ! -f .env ]; then
    echo "[run] .env not found; creating from .env.example"
    cp .env.example .env
  fi
}

# Read SR_VERSIONS from .env without sourcing (value contains spaces).
# docker-compose reads .env itself for compose-level substitution.
if [ -z "${SR_VERSIONS:-}" ] && [ -f .env ]; then
  SR_VERSIONS="$(grep -E '^SR_VERSIONS=' .env | tail -1 | cut -d= -f2- | tr -d '\"')"
fi
SR_VERSIONS="${SR_VERSIONS:-3.5.5 4.1.1}"

wait_starrocks_healthy() {
  echo "[run] StarRocks 헬스 대기..."
  local cid
  until cid="$($COMPOSE ps -q starrocks 2>/dev/null)"; [ -n "$cid" ] && \
        [ "$(docker inspect -f '{{.State.Health.Status}}' "$cid" 2>/dev/null)" = "healthy" ]; do
    sleep 5
  done
  echo "[run] StarRocks healthy."
}

cmd_up() {
  ensure_env
  echo "[run] building + starting stack..."
  $COMPOSE up -d --build "$@"
  echo "[run] waiting for services to become ready..."
  $COMPOSE exec -T "$RUNNER" python -m bench wait
  echo "[run] stack is up. Next: scripts/run.sh gen && scripts/run.sh smoke"
}

cmd_gen()    { $COMPOSE exec -T "$RUNNER" python -m bench gen "$@"; }
cmd_report() { $COMPOSE exec -T "$RUNNER" python -m bench report "$@"; }

# Run `smoke`/`bench` once per StarRocks version, swapping the container, accumulating
# into a single results dir, then build one merged report.
_run_matrix() {
  local mode="$1"; shift
  local ts resdir first=1
  ts="$(date +%Y%m%d-%H%M%S)"
  resdir="/results/${mode}-${ts}"
  echo "[run] ${mode}: StarRocks 버전 순회 = ${SR_VERSIONS}  -> ${resdir}"
  for ver in $SR_VERSIONS; do
    echo "============================================================"
    echo "[run] StarRocks ${ver} 로 교체/기동 (+ Spark 재시작으로 토큰 리프레시)"
    # Restart BOTH engines fresh per version so Polaris OAuth tokens stay valid for the
    # whole (~25min) run — long-lived sessions otherwise expire mid-run.
    STARROCKS_VERSION="$ver" $COMPOSE up -d --force-recreate --no-deps starrocks
    $COMPOSE up -d --force-recreate --no-deps spark
    wait_starrocks_healthy
    until [ "$(docker inspect -f '{{.State.Health.Status}}' "$($COMPOSE ps -q spark)" 2>/dev/null)" = "healthy" ]; do sleep 5; done
    echo "[run] Spark healthy."
    local genflag="--skip-gen"; [ "$first" = "1" ] && genflag=""
    $COMPOSE exec -T "$RUNNER" python -m bench "$mode" \
      --query-engine "starrocks-${ver}" --results-dir "$resdir" --no-report $genflag "$@"
    first=0
  done
  echo "[run] 리포트 생성..."
  $COMPOSE exec -T "$RUNNER" python -m bench report --results-dir "$resdir"
  echo "[run] 완료: ${resdir}/report.md"
}

cmd_smoke() { _run_matrix smoke "$@"; }
cmd_bench() { _run_matrix bench "$@"; }

cmd_shell()  { $COMPOSE exec "$RUNNER" bash; }
cmd_logs()   { $COMPOSE logs -f "$@"; }
cmd_ps()     { $COMPOSE ps; }

cmd_down() {
  echo "[run] stopping stack..."
  $COMPOSE down "$@"
}

cmd_help() {
  cat <<EOF
table-bench-mark — usage: scripts/run.sh <command> [args]

  up [--build]       build images, start the stack, wait for readiness
  gen [--no-upload]  pre-materialize seeded Parquet (NOT timed)
  smoke [opts]       tiny run of all 4 Iceberg scenarios, per StarRocks version
  bench [opts]       full run (100k x 10) of all scenarios x SR versions -> results/<ts>/
  report [--results-dir DIR]
  shell              open a shell in the bench-runner container
  logs [service]     tail logs
  ps                 list services
  down [-v]          stop the stack (-v also wipes volumes)

  smoke/bench loop over SR_VERSIONS (.env): currently "${SR_VERSIONS}"
  opts:  --candidate <iceberg-v3-mor|...>

examples:
  scripts/run.sh up
  scripts/run.sh bench
  scripts/run.sh bench --candidate iceberg-v3-mor
EOF
}

case "${1:-help}" in
  up)     shift; cmd_up "$@" ;;
  gen)    shift; cmd_gen "$@" ;;
  smoke)  shift; cmd_smoke "$@" ;;
  bench)  shift; cmd_bench "$@" ;;
  report) shift; cmd_report "$@" ;;
  shell)  shift; cmd_shell "$@" ;;
  logs)   shift; cmd_logs "$@" ;;
  ps)     shift; cmd_ps "$@" ;;
  down)   shift; cmd_down "$@" ;;
  help|-h|--help) cmd_help ;;
  *) echo "unknown command: $1"; echo; cmd_help; exit 1 ;;
esac
