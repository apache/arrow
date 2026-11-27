#!/usr/bin/env python
"""Performance gate for the pyarrow wheel (Python layer only).

Times the python/benchmarks modules against the installed pyarrow and
compares each benchmark's median with the committed reference
(perf_gate_reference.json). Machine-dependent, so the ceiling is 2x the
reference — a regression tripwire, not an absolute perf target. The
reference must be regenerated after intentional perf-affecting changes
(``perf_gate.py --write perf_gate_reference.json``).

Usage:
  perf_gate.py --gate            # exit 1 if any median > 2x the reference
  perf_gate.py --write OUT.json  # dump measured medians (new reference)

Scope: array_ops, convert_builtins, convert_pandas, streaming
(Python-facing layer; the C++ layer is identical across wheel flavors).
"""
import importlib
import importlib.util
import itertools
import json
import os
import statistics
import sys
import time

BENCH_MODULES = ["array_ops", "convert_builtins", "convert_pandas", "streaming"]
RUNS = 7
WARMUP = 1
GATE_FACTOR = 2.0
HERE = os.path.dirname(os.path.abspath(__file__))
BENCH_ROOT = os.path.normpath(os.path.join(HERE, "..", "benchmarks"))


def time_method(meth, args):
    for _ in range(WARMUP):
        meth(*args)
    samples = []
    for _ in range(RUNS):
        t0 = time.perf_counter()
        meth(*args)
        samples.append(time.perf_counter() - t0)
    return statistics.median(samples)


def cases(cls):
    # conbench-style parametrization: cartesian product of cls.params.
    if not getattr(cls, "params", None):
        yield ()
        return
    yield from itertools.product(*cls.params)


def load_bench_pkg():
    # Load the repo's benchmarks package without adding the source tree to
    # sys.path (that would shadow the installed wheel's pyarrow with the
    # in-tree one, which has no compiled modules).
    spec = importlib.util.spec_from_file_location(
        "benchmarks", os.path.join(BENCH_ROOT, "__init__.py"),
        submodule_search_locations=[BENCH_ROOT])
    pkg = importlib.util.module_from_spec(spec)
    sys.modules["benchmarks"] = pkg
    spec.loader.exec_module(pkg)


def collect():
    import pyarrow as pa
    load_bench_pkg()
    results, skipped = {}, {}
    for mod_name in BENCH_MODULES:
        try:
            mod = importlib.import_module(f"benchmarks.{mod_name}")
        except Exception as e:  # noqa: BLE001
            skipped[mod_name] = f"import: {e!r}"
            continue
        for cls_name in sorted(vars(mod)):
            cls = getattr(mod, cls_name)
            if not (isinstance(cls, type) and cls_name[0].isupper()):
                continue
            for m in sorted(n for n in vars(cls) if n.startswith("time_")):
                key = f"{mod_name}.{cls_name}.{m}"
                try:
                    per_case = []
                    for args in cases(cls):
                        obj = cls()
                        setup = getattr(obj, "setup", None) or getattr(
                            obj, "setUp", None)
                        if setup is not None:
                            setup(*args)
                        per_case.append(time_method(getattr(obj, m), args))
                    results[key] = statistics.median(per_case)
                    print(f"{key}: {results[key]:.6f}s", flush=True)
                except Exception as e:  # noqa: BLE001
                    skipped[key] = repr(e)
                    print(f"{key}: SKIPPED {e!r}", flush=True)
    return results, skipped


def main():
    if "--write" in sys.argv:
        out = sys.argv[sys.argv.index("--write") + 1]
        results, skipped = collect()
        json.dump({"results": results, "skipped": skipped},
                  open(out, "w"), indent=2, sort_keys=True)
        print(f"{len(results)} benchmarks, {len(skipped)} skipped -> {out}")
        return

    results, skipped = collect()
    ref_path = os.path.join(HERE, "perf_gate_reference.json")
    ref = json.load(open(ref_path))["results"]
    bad = []
    print(f"\n{'benchmark':60s} {'measured':>10s} {'reference':>10s}  ratio")
    for key, median in sorted(results.items()):
        r = ref.get(key)
        if r is None:
            print(f"{key:60s} {median:10.6f} {'(no ref)':>10s}  -")
            continue
        ratio = median / r
        flag = "  FAIL" if ratio > GATE_FACTOR else ""
        if ratio > GATE_FACTOR:
            bad.append(key)
        print(f"{key:60s} {median:10.6f} {r:10.6f}  {ratio:5.2f}x{flag}")
    if bad:
        print(f"\nPERF GATE FAIL: {len(bad)} benchmarks > {GATE_FACTOR}x "
              f"reference: {bad}")
        sys.exit(1)
    print(f"\nPERF GATE PASS: all {len(results)} benchmarks <= "
          f"{GATE_FACTOR}x reference")


if __name__ == "__main__":
    main()
