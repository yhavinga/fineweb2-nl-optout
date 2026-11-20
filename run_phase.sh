#!/bin/bash
# run_phase.sh <phase> <processes> [options of the phase]: one phase of fw2optout.py in <processes> processes at the
# lowest CPU and IO priority on this machine. Each process claims units until every unit of the phase is done (by it or
# by a process elsewhere). Logs: $LOGS/$TAG-<phase>-p<k>.log. Exits 0 only when every process did. STALE: seconds
# after which a claim without result is taken over (default that of fw2optout.py, 2 hours).
set -uo pipefail
PHASE=${1:?phase: hosts, fetch1, fetch2 or filter}
N=${2:?number of processes}
shift 2
HERE=$(cd "$(dirname "$0")" && pwd)
PY=${PY:-python3}
LOGS=${LOGS:-$HOME/logs}
TAG=${TAG:-fw2o}
case $PHASE in
  hosts) ARGS=(hosts) ;;
  fetch1) ARGS=(fetch --attempt 1) ;;
  fetch2) ARGS=(fetch --attempt 2 --concurrency 64) ;;
  filter) ARGS=(filter) ;;
  *) echo "unknown phase $PHASE"; exit 2 ;;
esac
mkdir -p "$LOGS"
export TOKENIZERS_PARALLELISM=false PYTHONUNBUFFERED=1
pids=()
for k in $(seq 1 "$N"); do
  nice -n 19 ionice -c 3 "$PY" "$HERE/fw2optout.py" ${STALE:+--stale "$STALE"} "${ARGS[@]}" "$@" > "$LOGS/$TAG-$PHASE-p$k.log" 2>&1 &
  pids+=($!)
  sleep 2
done
echo "$PHASE: started $N processes: ${pids[*]}"
fail=0
for p in "${pids[@]}"; do wait "$p" || fail=$((fail + 1)); done
echo "$PHASE: $N processes, $fail failed"
[ "$fail" = 0 ]
