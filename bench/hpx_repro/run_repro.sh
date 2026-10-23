#!/bin/bash
# Run one case of supervision_gaps_repro REPS times and print its R: lines.
#
#   run_repro.sh BIN OUTDIR CASE REPS [extra --r-* options]
#
# Each rep: fresh directory and ports, root first, then A and B in the order
# the case needs, SIGKILL of B (except the epoch case), then the A and root
# output. All processes run on this host.

set -u
BIN=$1 OUT=$2 CASE=$3 REPS=$4
shift 4
EXTRA="$*"
COMMON="--hpx:bind=none --hpx:ignore-batch-env --hpx:threads=4"

port() { python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1])'; }
wait_f() { for _ in $(seq 1200); do [ -f "$1" ] && return 0; sleep 0.05; done; return 1; }

for REP in $(seq 1 "$REPS"); do
  TAG=$(echo "$EXTRA" | tr -c 'a-z0-9' '_')
  D=$OUT/${CASE}${TAG:+_$TAG}_rep$REP
  mkdir -p "$D"
  RP=$(port)
  ARGS="--r-dir $D --r-case $CASE --r-poll-ms 500 $EXTRA"
  start() {
    "$BIN" --r-role "$1" --hpx:agas=localhost:$RP --hpx:hpx=localhost:$(port) \
      $COMMON $ARGS > "$D/$1.log" 2>&1 &
  }

  "$BIN" --r-role root --hpx:agas=localhost:$RP --hpx:hpx=localhost:$RP \
    --hpx:expect-connecting-localities $COMMON $ARGS > "$D/root.log" 2>&1 &
  ROOT=$!
  if ! wait_f "$D/root.ready"; then echo "rep $REP: root did not start"; kill -9 $ROOT; continue; fi

  if [ "$CASE" = leave ]; then
    # A and B join each other, so both must be up before either is ready.
    start a; A=$!
    start b; B=$!
    wait_f "$D/a.ready"; wait_f "$D/b.ready"
    sleep 3                      # several failure-detection sweeps in flight
    touch "$D/b.leave"
    for _ in $(seq 300); do kill -0 $B 2>/dev/null || break; sleep 0.1; done
    if kill -0 $B 2>/dev/null; then RC=hung; else wait $B; RC=$?; fi
    echo "== $CASE $EXTRA rep $REP"
    grep -h '^R:' "$D/a.log" "$D/b.log"
    case $RC in
      0) echo "RESULT leave $EXTRA rep $REP: B exited cleanly (rc 0)" ;;
      134) echo "RESULT leave $EXTRA rep $REP: B aborted (rc 134, SIGABRT); $(grep -m1 -o 'The runtime system is not active' "$D/b.log")" ;;
      *) echo "RESULT leave $EXTRA rep $REP: B rc=$RC" ;;
    esac
    kill -9 $A $B $ROOT 2>/dev/null
    wait 2>/dev/null
    continue
  fi

  if [ "$CASE" = observer ]; then
    start a; A=$!; wait_f "$D/a.ready"
    start b; B=$!; wait_f "$D/b.ready"
    sleep 1
  else
    start b; B=$!; wait_f "$D/b.ready"
    start a; A=$!; wait_f "$D/a.ready"
    if [ "$CASE" = inflight ]; then wait_f "$D/a.dispatched"; sleep 0.3; else sleep 0.5; fi
  fi

  if [ "$CASE" != epoch ]; then
    kill -9 $B
    touch "$D/b.killed"
  fi
  for _ in $(seq 900); do kill -0 $A 2>/dev/null || break; sleep 0.1; done

  echo "== $CASE $EXTRA rep $REP"
  grep -h '^R:' "$D/b.log" "$D/a.log" "$D/root.log" | grep -v ' done$'
  kill -9 $A $B $ROOT 2>/dev/null
  wait 2>/dev/null
done
