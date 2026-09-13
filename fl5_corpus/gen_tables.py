import csv, math, sys
from collections import OrderedDict

OUT = sys.argv[1]
rows = list(csv.DictReader(open("fl5_corpus_x86.csv")))
ARMS = ["seq_scal", "seq_simd", "intlv", "fl_unpk", "fl_tpos"]
POINTS = ["L1", "L2", "DRAM"]

by_ds = OrderedDict()
for r in rows:
    by_ds.setdefault(r["dataset"], {})[r["point"]] = r

def gm(vals):
    return math.exp(sum(math.log(v) for v in vals) / len(vals))

# ---- geomeans per point ----
G = {}
for p in POINTS:
    sel = [by_ds[d][p] for d in by_ds if p in by_ds[d]]
    G[p] = {a: gm([float(r[a]) for r in sel]) for a in ARMS}
    G[p]["n"] = len(sel)
    # sign counts: how many datasets have the ratio above 1.0
    G[p]["win_intlv"] = sum(1 for r in sel if float(r["intlv"]) > float(r["seq_simd"]))
    G[p]["win_tpos"]  = sum(1 for r in sel if float(r["fl_tpos"]) > float(r["seq_simd"]))
    G[p]["min_intlv"] = min(float(r["intlv"]) / float(r["seq_simd"]) for r in sel)
    G[p]["max_intlv"] = max(float(r["intlv"]) / float(r["seq_simd"]) for r in sel)

NDS = G["L2"]["n"]
CAL = {"L1": 1.27, "L2": 1.27, "DRAM": 1.00}

out = []
w = out.append

w('\n<h2 id="s8">8. Results: the geomeans</h2>\n')
w('<p>Geometric mean across %d real columns, GiB/s measured on <em>output</em> bytes (4 bytes per\n'
  'value decoded), higher is better. Ratios are against <code>seq_simd</code>, the arm that\n'
  'represents what Parquet decodes at today.</p>\n' % NDS)
w('<div class="tablewrap">\n<table>\n')
w('<caption>Table 2. Geometric mean over %d columns. The <code>fl_unpk/intlv</code> column is the\n'
  'self-check described in Table 1: two arms running identical kernel code on identically-shaped\n'
  'grids, which must and do agree. <b>The ratios in this table are raw and the L1/L2 rows are\n'
  'inflated &mdash; see &sect;10 before quoting them.</b></caption>\n' % NDS)
w('<thead>\n<tr><th>Working set</th>')
for a in ARMS: w('<th class="num">%s</th>' % a)
w('<th class="num">intlv<br>/seq_simd</th><th class="num">fl_tpos<br>/seq_simd</th>'
  '<th class="num">fl_unpk<br>/intlv</th></tr>\n</thead>\n<tbody>\n')
LBL = {"L1": "L1 &mdash; 16 KiB", "L2": "L2 &mdash; 400 KiB", "DRAM": "DRAM &mdash; 32 MiB"}
for p in POINTS:
    g = G[p]
    w('<tr><td>%s</td>' % LBL[p])
    for a in ARMS: w('<td class="num">%.1f</td>' % g[a])
    w('<td class="num"><b>%.2fx</b></td><td class="num">%.2fx</td><td class="num">%.3fx</td></tr>\n'
      % (g["intlv"]/g["seq_simd"], g["fl_tpos"]/g["seq_simd"], g["fl_unpk"]/g["intlv"]))
w('</tbody>\n</table>\n</div>\n')

w('<p>Two things in that table matter more than the headline ratios.</p>\n')
w('<p><b>First, the DRAM row.</b> At a 32 MiB working set every layout converges: %.1f, %.1f, %.1f,\n'
  '%.1f, %.1f GiB/s across the five arms, a spread of %.0f%% between the fastest and slowest of the\n'
  'four SIMD arms. The kernel is no longer instruction-bound, it is waiting on memory, and no\n'
  'arrangement of bits changes how fast DRAM delivers them. Any claim about layout is implicitly a\n'
  'claim about cache-resident decoding.</p>\n'
  % (G["DRAM"]["seq_scal"], G["DRAM"]["seq_simd"], G["DRAM"]["intlv"], G["DRAM"]["fl_unpk"],
     G["DRAM"]["fl_tpos"],
     100 * (max(G["DRAM"][a] for a in ARMS[1:]) / min(G["DRAM"][a] for a in ARMS[1:]) - 1)))
w('<p><b>Second, the scalar column.</b> <code>seq_simd</code> beats <code>seq_scal</code> by\n'
  '%.2fx at L1 and %.2fx at L2. That gap is the one the FastLanes paper\'s "order of magnitude" is\n'
  'largely measuring &mdash; it is the cost of not vectorising at all, and Arrow already collected\n'
  'it years ago. The layout question is what remains <em>after</em> that, and it is a much smaller\n'
  'number.</p>\n' % (G["L1"]["seq_simd"]/G["L1"]["seq_scal"], G["L2"]["seq_simd"]/G["L2"]["seq_scal"]))

w('<h3>How consistent is it across columns?</h3>\n')
w('<div class="tablewrap">\n<table>\n')
w('<caption>Table 3. Per-column consistency of the layout effect, raw. "Wins" counts columns where\n'
  'the arm is strictly faster than <code>seq_simd</code>.</caption>\n')
w('<thead>\n<tr><th>Working set</th><th class="num">intlv wins</th><th class="num">fl_tpos wins</th>'
  '<th class="num">min intlv/seq_simd</th><th class="num">max intlv/seq_simd</th></tr>\n</thead>\n<tbody>\n')
for p in POINTS:
    g = G[p]
    w('<tr><td>%s</td><td class="num">%d / %d</td><td class="num">%d / %d</td>'
      '<td class="num">%.2fx</td><td class="num">%.2fx</td></tr>\n'
      % (LBL[p], g["win_intlv"], g["n"], g["win_tpos"], g["n"], g["min_intlv"], g["max_intlv"]))
w('</tbody>\n</table>\n</div>\n')
w('<p>At L1 the interleaved container is faster on every single column; at L2 on all but one; at\n'
  'DRAM on barely half, which is what "bandwidth-bound" looks like in a sign test. The direction of\n'
  'the effect is not in doubt at L1 and L2. Its magnitude is what &sect;10 corrects.</p>\n')

# ---- big per-dataset table ----
w('\n<h2 id="s9">9. Results: %d columns &times; L1 / L2 / DRAM</h2>\n' % NDS)
w('<p>Every measurement, unaggregated. <code>W</code> is the mean per-block bit width the encoder\n'
  'chose for that column and <code>CR</code> its compression ratio &mdash; together they explain most\n'
  'of the variation between rows. Speeds are GiB/s on output bytes; the best of the four SIMD arms in\n'
  'each row is shown in bold. Ratios are raw; see &sect;10.</p>\n')
w('<div class="tablewrap">\n<table>\n')
w('<caption>Table 4. All %d columns at all three working sets. Sources: ClickBench, TPC-H, and the\n'
  'Public BI benchmark corpus. Rows are ordered by mean bit width, because that is the variable the\n'
  'effect tracks (&sect;11).</caption>\n' % NDS)
w('<thead>\n<tr><th>Column</th><th class="num">W</th><th class="num">CR</th><th>Set</th>')
for a in ARMS: w('<th class="num">%s</th>' % a)
w('<th class="num">intlv<br>/seq_simd</th><th class="num">fl_tpos<br>/seq_simd</th></tr>\n</thead>\n<tbody>\n')

order = sorted(by_ds, key=lambda d: (float(by_ds[d]["L2"]["avg_bit_width"]), d))
for d in order:
    pts = by_ds[d]
    ref = pts["L2"]
    for k, p in enumerate(POINTS):
        if p not in pts: continue
        r = pts[p]
        w('<tr>')
        if k == 0:
            w('<td rowspan="3">%s</td><td class="num" rowspan="3">%.1f</td>'
              '<td class="num" rowspan="3">%.2f</td>'
              % (d, float(ref["avg_bit_width"]), float(ref["cr"])))
        w('<td>%s</td>' % p)
        vals = {a: float(r[a]) for a in ARMS}
        best = max(vals[a] for a in ARMS[1:])
        for a in ARMS:
            v = vals[a]
            cell = '%.1f' % v
            if a != "seq_scal" and v == best: cell = '<b>%s</b>' % cell
            w('<td class="num">%s</td>' % cell)
        w('<td class="num">%.2fx</td><td class="num">%.2fx</td></tr>\n'
          % (vals["intlv"]/vals["seq_simd"], vals["fl_tpos"]/vals["seq_simd"]))
w('</tbody>\n</table>\n</div>\n')

open(OUT, "a").write("".join(out))
print("appended %d bytes; %d datasets" % (sum(len(x) for x in out), NDS))
