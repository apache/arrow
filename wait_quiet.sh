#!/usr/bin/env bash
# Block until the box is quiet enough to time on.
#
# Three conditions, because loadavg alone is not enough: it is a 1-minute
# average and lags, and a benchmark that has just started shows a low load while
# already owning a core. A parallel session shares this box, so "no compiler
# running" and "no benchmark running" both have to hold.
#
# NOTE on pgrep: `pgrep -c` prints 0 AND exits 1 when nothing matches, so the
# idiom `$(pgrep -c x || echo 0)` yields the two-line string "0\n0" and every
# later [ -eq ] test dies with "integer expression expected". Assign, then
# default.
set -uo pipefail
MAX=${MAX:-60}          # attempts
SLEEP=${SLEEP:-30}
for i in $(seq 1 "$MAX"); do
  L=$(awk '{print $1}' /proc/loadavg); LI=${L%.*}
  C=$(pgrep -c cc1plus 2>/dev/null); C=${C:-0}
  B=$(pgrep -fc pfor-comparison-benchmark 2>/dev/null); B=${B:-0}
  if [ "${LI:-9}" -lt 2 ] && [ "$C" -eq 0 ] && [ "$B" -eq 0 ]; then
    echo "QUIET after $((i-1)) waits: load=$L cc1plus=$C bench=$B"; exit 0
  fi
  [ $((i % 4)) -eq 1 ] && echo "  waiting: load=$L cc1plus=$C bench=$B"
  sleep "$SLEEP"
done
echo "STILL_BUSY: load=$(awk '{print $1}' /proc/loadavg)"; exit 1
