#!/usr/bin/env python3
"""Turn a register-width sweep tarball into the layout tables.

Reads the combined_results.json produced by x86_register_width_sweep.sh and
prints, per build register width, the comparison the layout decision actually
rests on: one baseline and three grid variants, across every working-set size and
every column, plus the geomean over columns.

    python3 pfor_layout_tables.py combined_results.json [--stat median]

The four benchmarks, and why all four are needed:

    seq       BM_PforPlainSeqDecode               continuous layout, delta
                                                  declined.  THE BASELINE.
    intlv     BM_InterleavedPforDecode            grid filled in file order.
                                                  CONTROL -- unshippable, it
                                                  prices the grid's unpacking
                                                  with no permutation to pay.
    fl_unpk   BM_InterleavedPforFlOrderRawDecode  grid filled the paper's way,
                                                  handed back unpermuted.
                                                  CONTROL -- violates the
                                                  positional contract.
    fl_tpos   BM_InterleavedPforFlOrderDecode     same grid, permuted back to
                                                  file order by the fused
                                                  in-register transpose.
                                                  THE CANDIDATE, and the only
                                                  ratio that answers it.

Read the columns in this order.  fl_unpk/seq is what the layout wins.  The tax
column, fl_unpk/fl_tpos, is what the positional contract charges to collect it.
fl_tpos/seq is the product of the two and is the only number that decides
anything.  A flat fl_tpos/seq with a tax near 1.0 means the grid is no cheaper;
a flat fl_tpos/seq with a large tax means the grid is much cheaper and the
permutation spends the whole win.  Those are different findings with different
fixes, and they are indistinguishable without fl_unpk.
"""
import argparse
import collections
import json
import math
import re
import sys

BENCHES = [
    ("seq", "BM_PforPlainSeqDecode"),
    ("intlv", "BM_InterleavedPforDecode"),
    ("fl_unpk", "BM_InterleavedPforFlOrderRawDecode"),
    ("fl_tpos", "BM_InterleavedPforFlOrderDecode"),
]
NAME_FOR = {bench: name for name, bench in BENCHES}

# Decoded output bytes = 4 * num_values.  Labels are what the ladder was chosen
# against, not what any particular machine has; a run on a different cache
# hierarchy should be read against its own lscpu.
FOOTPRINT = {
    4096: "16 KiB",
    102400: "400 KiB",
    393216: "1.5 MiB",
    1048576: "4 MiB",
    8388608: "32 MiB",
}

NAME_RE = re.compile(r"^(BM_[A-Za-z0-9]+)/([^/]+)/(\d+)(?:_(\w+))?$")


def geomean(xs):
    xs = [x for x in xs if x > 0]
    return math.exp(sum(math.log(x) for x in xs) / len(xs)) if xs else float("nan")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("json_path")
    ap.add_argument(
        "--stat",
        default="median",
        help="aggregate to read: median (default) or mean. median is less "
        "sensitive to a single thermally-throttled repetition.",
    )
    args = ap.parse_args()

    with open(args.json_path) as f:
        data = json.load(f)

    # (level, name, dataset, nvalues) -> GiB/s ; and the CV alongside, because a
    # ratio built out of two noisy cells is not evidence.
    gbps = {}
    cv = {}
    for b in data.get("benchmarks", []):
        m = NAME_RE.match(b.get("run_name") or b.get("name", ""))
        if not m:
            continue
        bench, dataset, n, _ = m.groups()
        name = NAME_FOR.get(bench)
        if name is None:
            continue
        level = b.get("build_simd_level", b.get("simd_level", "?"))
        key = (level, name, dataset, int(n))
        agg = b.get("aggregate_name")
        if agg == args.stat:
            bps = b.get("bytes_per_second")
            if bps:
                gbps[key] = bps / (1 << 30)
        elif agg == "cv":
            cv[key] = b.get("bytes_per_second", 0.0)

    if not gbps:
        sys.exit(
            "no layout benchmark records found. If this tarball predates the "
            "footprint ladder it has only the 102400 point, and\n"
            "BM_InterleavedPforFlOrderRawDecode will be missing entirely -- "
            "that run cannot produce these tables."
        )

    # A missing benchmark is the single most likely thing to be wrong with a returned
    # tarball, and it is silent: every ratio involving it just prints nan. Say
    # so once, loudly, at the top.
    present = {k[1] for k in gbps}
    missing = [name for name, _ in BENCHES if name not in present]
    if missing:
        bench_for = dict(BENCHES)
        print("!" * 104)
        print("INCOMPLETE RUN -- missing: " + ", ".join(missing))
        for name in missing:
            print(f"    {name:<9} ({bench_for[name]})")
        print()
        print("  Every ratio involving a missing one below is nan. If fl_unpk is")
        print("  the missing one, this tarball predates that control and cannot")
        print("  separate 'the grid is no cheaper' from 'the grid is much cheaper")
        print("  and the permutation spends the win' -- rebuild and re-run.")
        print("!" * 104)
        print()

    sizes_seen = {k[3] for k in gbps}
    if len(sizes_seen) == 1:
        print("!" * 104)
        print(f"SINGLE WORKING-SET SIZE ONLY ({sorted(sizes_seen)[0]} values). This")
        print("  tarball predates the footprint ladder. The grid's advantage is a")
        print("  compute effect, and whether it survives at a given footprint is")
        print("  what the ladder is for -- rebuild and re-run.")
        print("!" * 104)
        print()

    levels = sorted({k[0] for k in gbps})
    for level in levels:
        sizes = sorted({k[3] for k in gbps if k[0] == level})
        datasets = []
        for k in gbps:
            if k[0] == level and k[2] not in datasets:
                datasets.append(k[2])
        datasets.sort()

        print("=" * 104)
        print(f"BUILD REGISTER WIDTH: {level}")
        print("=" * 104)
        if level == "AVX512":
            print(
                "  NOTE: not like-for-like. The sequential decoder is capped at "
                "AVX2 in bpacking.cc, so this\n"
                "  column widens the grid benchmarks only -- except the candidate, "
                "pinned at 256 by its own __m256i."
            )
        print(
            "  seq = continuous [BASELINE] | intlv, fl_unpk = CONTROLS, not "
            "shippable | fl_tpos = CANDIDATE"
        )
        print("  tax = fl_unpk/fl_tpos = what the positional contract charges")
        print()

        def cell(name, ds, n):
            return gbps.get((level, name, ds, n))

        hdr = (
            f"{'footprint':<10} | {'seq':>8} {'intlv':>8} {'fl_unpk':>8} "
            f"{'fl_tpos':>8} | {'int/seq':>8} {'unpk/seq':>8} "
            f"{'tpos/seq':>8} {'tax':>6}"
        )
        print("-" * 104)
        print("PER-FOOTPRINT GEOMEAN OVER {} COLUMNS".format(len(datasets)))
        print("-" * 104)
        print(hdr)
        print("-" * 104)
        gm = {}
        for n in sizes:
            g = {}
            for name, _ in BENCHES:
                vals = [cell(name, ds, n) for ds in datasets]
                vals = [v for v in vals if v]
                g[name] = geomean(vals) if vals else float("nan")
            gm[n] = g
            print(
                f"{FOOTPRINT.get(n, str(n)):<10} | {g['seq']:>8.1f} "
                f"{g['intlv']:>8.1f} {g['fl_unpk']:>8.1f} {g['fl_tpos']:>8.1f} | "
                f"{g['intlv'] / g['seq']:>8.3f} {g['fl_unpk'] / g['seq']:>8.3f} "
                f"{g['fl_tpos'] / g['seq']:>8.3f} "
                f"{g['fl_unpk'] / g['fl_tpos']:>6.3f}"
            )
        print()

        for n in sizes:
            print("-" * 104)
            print(f"PER-COLUMN  @  {FOOTPRINT.get(n, str(n))}  ({n} values)")
            print("-" * 104)
            print(
                f"{'column':<24} | {'seq':>8} {'intlv':>8} {'fl_unpk':>8} "
                f"{'fl_tpos':>8} | {'int/seq':>8} {'unpk/seq':>8} "
                f"{'tpos/seq':>8} {'tax':>6} {'maxCV':>6}"
            )
            print("-" * 104)
            for ds in datasets:
                v = {name: cell(name, ds, n) for name, _ in BENCHES}
                if not v["seq"]:
                    continue
                # A missing one prints as nan rather than dropping the row: a row
                # that vanishes looks like a column that was not run.
                v = {a: (x if x else float("nan")) for a, x in v.items()}
                worst = max(
                    (cv.get((level, name, ds, n), 0.0) for name, _ in BENCHES),
                    default=0.0,
                )
                print(
                    f"{ds:<24} | {v['seq']:>8.1f} {v['intlv']:>8.1f} "
                    f"{v['fl_unpk']:>8.1f} {v['fl_tpos']:>8.1f} | "
                    f"{v['intlv'] / v['seq']:>8.3f} "
                    f"{v['fl_unpk'] / v['seq']:>8.3f} "
                    f"{v['fl_tpos'] / v['seq']:>8.3f} "
                    f"{v['fl_unpk'] / v['fl_tpos']:>6.3f} "
                    f"{worst * 100:>5.1f}%"
                )
            g = gm[n]
            print("-" * 104)
            print(
                f"{'GEOMEAN':<24} | {g['seq']:>8.1f} {g['intlv']:>8.1f} "
                f"{g['fl_unpk']:>8.1f} {g['fl_tpos']:>8.1f} | "
                f"{g['intlv'] / g['seq']:>8.3f} {g['fl_unpk'] / g['seq']:>8.3f} "
                f"{g['fl_tpos'] / g['seq']:>8.3f} "
                f"{g['fl_unpk'] / g['fl_tpos']:>6.3f}"
            )
            print()

        # The controls must agree with each other: same kernel, same wire
        # bytes, only the grid fill differs. A gap between them is a bug or a
        # noise floor, and either way it bounds what the other ratios can mean.
        print("-" * 104)
        print("CONTROL CHECK: intlv vs fl_unpk must be equal (same kernel, "
              "same bytes, different fill)")
        print("-" * 104)
        for n in sizes:
            g = gm[n]
            r = g["fl_unpk"] / g["intlv"]
            flag = "" if 0.97 <= r <= 1.03 else "   <-- OFF BY >3%, investigate"
            print(f"  {FOOTPRINT.get(n, str(n)):<10} fl_unpk/intlv = {r:.4f}{flag}")
        print()


if __name__ == "__main__":
    main()
