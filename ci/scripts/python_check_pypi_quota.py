#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Check whether a new PyPI release would exceed the project size quota.

Fetches the current project size from the PyPI JSON API and adds the
sizes of the artifacts about to be uploaded. Files whose names already
exist on PyPI are excluded, since PyPI rejects re-uploads of the same
filename and they do not grow the project's storage footprint.
"""

import argparse
import json
import sys
import urllib.error
import urllib.request
from pathlib import Path

PYPI_REQUEST_TIMEOUT_SECONDS = 30

# PyPI's default per-project storage quota.
PYPI_QUOTA_BYTES = 80 * 1024 ** 3

WHEEL_AND_SDIST_PATTERNS = ("*.whl", "*.tar.gz")


def format_bytes(n):
    gib = n / (1024 ** 3)
    return f"{n:,} bytes ({gib:.3f} GiB)"


def fetch_pypi_file_sizes(project):
    url = f"https://pypi.org/pypi/{project}/json"
    try:
        with urllib.request.urlopen(
                url, timeout=PYPI_REQUEST_TIMEOUT_SECONDS) as response:
            data = json.load(response)
    except urllib.error.HTTPError as exc:
        raise SystemExit(
            f"error: PyPI request for {project} failed: "
            f"HTTP {exc.code} {exc.reason}")
    except urllib.error.URLError as exc:
        raise SystemExit(
            f"error: PyPI request for {project} failed: {exc.reason}")

    seen = {}
    for files in data.get("releases", {}).values():
        for info in files:
            filename = info.get("filename")
            if filename is None or filename in seen:
                continue
            seen[filename] = info.get("size") or 0
    return seen


def collect_local_files(path):
    root = Path(path)
    if not root.is_dir():
        raise SystemExit(f"error: {root} is not a directory")
    artifacts = []
    for pattern in WHEEL_AND_SDIST_PATTERNS:
        for artifact in root.glob(pattern):
            artifacts.append((artifact.name, artifact.stat().st_size))
    return sorted(artifacts)


def load_assets_json(source):
    if source == "-":
        data = json.load(sys.stdin)
    else:
        with open(source) as f:
            data = json.load(f)
    return sorted((item["name"], item["size"]) for item in data)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument(
        "--path",
        help="Directory containing the wheels and sdist to be uploaded.")
    source.add_argument(
        "--assets-json",
        help="Path to a JSON file (or - for stdin) with a list of "
             "{name, size} objects describing the artifacts to be "
             "uploaded.")
    parser.add_argument(
        "--project", default="pyarrow",
        help="PyPI project name (default: pyarrow).")
    args = parser.parse_args()

    if args.path is not None:
        artifacts = collect_local_files(args.path)
        source_desc = args.path
    else:
        artifacts = load_assets_json(args.assets_json)
        source_desc = args.assets_json
    if not artifacts:
        raise SystemExit(
            f"error: no wheels or sdist found in {source_desc}")

    pypi_files = fetch_pypi_file_sizes(args.project)
    current_size = sum(pypi_files.values())

    new_size = 0
    new_count = 0
    skipped = []
    for name, size in artifacts:
        if name in pypi_files:
            skipped.append(name)
            continue
        new_size += size
        new_count += 1

    projected = current_size + new_size

    print(f"Project: {args.project}")
    print(f"Quota:            {format_bytes(PYPI_QUOTA_BYTES)}")
    print(f"Current on PyPI:  {format_bytes(current_size)} "
          f"across {len(pypi_files)} files")
    print(f"New to upload:    {format_bytes(new_size)} "
          f"across {new_count} files")
    if skipped:
        print(f"Skipped (already on PyPI): {len(skipped)} files")
        for name in skipped:
            print(f"  - {name}")
    print(f"Projected total:  {format_bytes(projected)}")
    headroom = PYPI_QUOTA_BYTES - projected
    print(f"Headroom:         {format_bytes(headroom)}")

    if projected > PYPI_QUOTA_BYTES:
        over = projected - PYPI_QUOTA_BYTES
        print(
            f"\nERROR: projected PyPI size exceeds the quota by "
            f"{format_bytes(over)}.",
            file=sys.stderr)
        sys.exit(1)

    print("\nOK: projected PyPI size is within the quota.")


if __name__ == "__main__":
    main()
