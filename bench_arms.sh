# Shared arm sets and column set for pfor_comparison_benchmark drivers.
# Sourced, not executed:  . "$(dirname "$0")/bench_arms.sh"
#
# WHY THIS FILE EXISTS
#   Four drivers each carried their own copy of the column list and the arm
#   regex, and all four copies named the same wrong pair: BM_PforDecode (the
#   production decoder, which patches exceptions) against BM_InterleavedPforDecode
#   and BM_InterleavedPforFlOrderDecode (a standalone path with no exception
#   handling at all). A ratio between those is not a layout result -- the
#   interleaved side is doing less work. The correct pair had already landed in
#   the same benchmark file when those drivers were written.
#
#   Keeping the sets here means the rule below is stated once and cannot drift
#   between drivers.
#
# WHICH ARMS MAY BE COMPARED WITH WHICH
#   Only within a group. Every arm in a group runs the same code with the same
#   exception handling and the same destination policy, so the named variable is
#   the only difference.
#
#   ARMS_LAYOUT  sequential vs interleaved container, both through the
#                production encoder and decoder with delta declined on both
#                sides. This is the pair a layout verdict is read from.
#
#   ARMS_ORDER   file order vs the paper's lane assignment inside the
#                interleaved container. These run arrow/util/fastlanes/
#                interleaved_pfor.h, which has no exception handling anywhere in
#                it, so the file-order arm in this group is their only valid
#                baseline. Never pair one of these against a production arm.
#                The production format has no lane-assignment mode, which is why
#                this group exists outside it.
#
#   ARMS_DEST    whole-output materialization vs a reused block-sized
#                destination, at each layout. Answers what the output buffer
#                costs, not what the layout costs.
#
#                ARMS_DEST arms arrive with the output-buffer-reuse change and
#                are absent from a tree without it; a filter naming them then
#                matches nothing, which google-benchmark reports as no
#                benchmarks to run.
#
#   ARMS_SHIPPED the shipping default, planner free to difference a vector. Not
#                a layout arm: on a sorted or correlated column it deltas, and
#                the decode then also pays a serial prefix sum.
#
# COLUMN SET
#   Changing it changes the numbers, because the count of columns a filter
#   admits decides how many corpora are allocated before the timed buffer. Hold
#   it fixed across anything meant to be compared.

COLS=${COLS:-'TpcdsSoldDateSk|TpcdsStoreSk|TpcdsItemSk|TpcdsQuantity|EventDate|ClientIP|CounterID|SortedKeys|MonotoneRowId|RandomWalk'}

ARMS_LAYOUT='BM_(PforPlainSeqDecode|PforPlainInterleavedDecode)'
ARMS_ORDER='BM_(InterleavedPforDecode|InterleavedPforFlOrderRawDecode|InterleavedPforFlOrderDecode)'
ARMS_DEST='BM_Pfor(Whole|Reuse)(Seq|Interleaved)Decode'
ARMS_SHIPPED='BM_Pfor(64)?Decode'

# Build a google-benchmark filter from one or more arm sets.
#   FILTER=$(bench_filter "$ARMS_LAYOUT" "$ARMS_ORDER")
# Groups are unioned so one process can time several of them -- that is fine and
# is in fact preferred, since arms timed in one process share the machine state.
# What is not fine is dividing a number from one group by a number from another.
bench_filter() {
  local joined="" a
  for a in "$@"; do
    joined="${joined:+$joined|}${a}"
  done
  printf '(%s)/(%s)/' "$joined" "$COLS"
}
