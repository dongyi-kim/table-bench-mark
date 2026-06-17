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

_wait_healthy() {  # $1 = service name, $2 = timeout secs (default 300) -> returns 1 on timeout
  local svc="$1" timeout="${2:-300}" waited=0 cid
  while :; do
    cid="$($COMPOSE ps -q "$svc" 2>/dev/null)"
    [ -n "$cid" ] && [ "$(docker inspect -f '{{.State.Health.Status}}' "$cid" 2>/dev/null)" = "healthy" ] && return 0
    [ "$waited" -ge "$timeout" ] && { echo "[run]   WARN: $svc 가 ${timeout}s 내 healthy 안 됨"; return 1; }
    sleep 5; waited=$((waited+5))
  done
}

# Per-combo isolation. Spark is recreated every combo (fresh JVM clears accumulated driver
# memory — the main cause of mid-run instability). StarRocks is only recreated when it has
# actually gone unhealthy (its force-recreate + FE recovery is slow), otherwise left running.
_is_healthy() {  # $1 = service
  local cid; cid="$($COMPOSE ps -q "$1" 2>/dev/null)"
  [ -n "$cid" ] && [ "$(docker inspect -f '{{.State.Health.Status}}' "$cid" 2>/dev/null)" = "healthy" ]
}
restart_engines() {
  local ver="$1"
  # Spark-only run: StarRocks excluded. Fresh Spark JVM per combo clears accumulated driver
  # memory. Retry startup so a failed launch never hangs the whole matrix.
  local attempt
  for attempt in 1 2 3; do
    $COMPOSE up -d --force-recreate --no-deps spark >/dev/null 2>&1
    echo "[run]   Spark 재시작(시도 $attempt) → healthy 대기..."
    if _wait_healthy spark 240; then echo "[run]   Spark healthy."; return 0; fi
  done
  echo "[run]   WARN: Spark 기동 반복 실패 — 이 조합은 실패로 기록될 수 있음"
  return 0
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

# Run `smoke`/`bench` over the full matrix. Data is generated ONCE; then each
# (StarRocks version × scenario × compaction mode) combo runs after a fresh engine restart
# (isolation), accumulating into one results dir, then a single merged report.
_run_matrix() {
  local mode="$1"; shift
  local ts resdir
  ts="$(date +%Y%m%d-%H%M%S)"
  resdir="/results/${mode}-${ts}"

  # scenarios from candidate YAMLs; compaction modes from benchmark.yaml.
  local cands modes
  cands="$(ls benchmark/config/candidates/*.yaml | sed 's#.*/##; s#\.yaml$##')"
  modes="$(awk '/^compaction_modes:/{f=1;next} f&&/^[[:space:]]*-[[:space:]]/{print $2} f&&/^[^[:space:]-]/{f=0}' benchmark/config/benchmark.yaml)"

  echo "[run] ${mode} -> ${resdir}"
  echo "[run] 시드 데이터 생성(측정 제외)..."
  local genargs=""; [ "$mode" = "smoke" ] && genargs="--smoke"
  $COMPOSE exec -T "$RUNNER" python -m bench gen $genargs

  for ver in $SR_VERSIONS; do
    for cand in $cands; do
      for comp in $modes; do
        echo "============================================================"
        echo "[run] ${cand} / ${comp} / starrocks-${ver}"
        restart_engines "$ver"
        $COMPOSE exec -T "$RUNNER" python -m bench "$mode" \
          --candidate "$cand" --compaction "$comp" \
          --query-engine "starrocks-${ver}" --results-dir "$resdir" --no-report --skip-gen "$@"
      done
    done
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
