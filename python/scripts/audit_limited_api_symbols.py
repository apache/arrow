#!/usr/bin/env python
"""Audit undefined Python C-API symbols of built pyarrow extensions.

Checks that every Py* symbol a pyarrow extension imports is exported by the
given reference libpython (CPython 3.11 for cp311-abi3 wheels). Fails on any
miss: such a symbol would break the wheel on the oldest supported
interpreter.

Usage:
  audit_limited_api_symbols.py <libpython3.11.so> <extension.so|pyd> [...]
"""
import re
import subprocess
import sys


def undefined_syms(binary):
    if binary.endswith((".pyd", ".dll", ".exe")):
        out = subprocess.run(
            ["objdump", "-p", binary], capture_output=True, text=True, check=True
        ).stdout
        return set(re.findall(r"^\s+\[\s*\d+\] (?:Py|_Py)\w+$", out, re.M))
    syms = set()
    out = subprocess.run(
        ["readelf", "--dyn-syms", binary], capture_output=True, text=True, check=True
    ).stdout
    for line in out.splitlines():
        f = line.split()
        if len(f) >= 8 and f[6] == "UND" and re.fullmatch(r"(?:Py|_Py)\w+", f[7]):
            syms.add(f[7])
    return syms


def exported_syms(libpython):
    out = subprocess.run(
        ["readelf", "--dyn-syms", libpython], capture_output=True, text=True, check=True
    ).stdout
    res = set()
    for line in out.splitlines():
        f = line.split()
        if len(f) >= 8 and f[4] == "GLOBAL" and f[6] != "UND":
            res.add(f[7])
    return res


def main():
    if len(sys.argv) < 3:
        sys.exit(__doc__)
    libpython, *exts = sys.argv[1:]
    available = exported_syms(libpython)
    missing = set()
    for ext in exts:
        missing |= undefined_syms(ext) - available
    if missing:
        print(f"FAIL: {len(missing)} Py* symbols not exported by {libpython}:")
        print("\n".join(sorted(missing)))
        sys.exit(1)
    print(f"OK: all Py* symbols used by {len(exts)} extensions exist in {libpython}")


if __name__ == "__main__":
    main()
