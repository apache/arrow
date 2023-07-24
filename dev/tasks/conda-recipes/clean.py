import subprocess
from typing import Set

import json
import pandas as pd
import sys

from packaging.version import Version


VERSIONS_TO_KEEP = 5
DELETE_BEFORE = pd.Timestamp.now() - pd.Timedelta(days=30)

PLATFORMS = [
    "linux-64",
    "linux-aarch64",
    "linux-ppc64le",
    "osx-64",
    "osx-arm64",
    "win-64",
]


class CommandFailedException(Exception):

    def __init__(self, cmdline, output):
        self.cmdline = cmdline
        self.output = output


def run_command(cmdline, **kwargs):
    kwargs.setdefault('capture_output', True)
    p = subprocess.run(cmdline, **kwargs)
    if p.returncode != 0:
        print(f"Command {cmdline} returned non-zero exit status "
              f"{p.returncode}", file=sys.stderr)
        output = ""
        if p.stdout:
            print("Stdout was:\n" + "-" * 70, file=sys.stderr)
            output = p.stdout.decode().rstrip()
            print(output, file=sys.stderr)
            print("-" * 70, file=sys.stderr)
        if p.stderr:
            print("Stderr was:\n" + "-" * 70, file=sys.stderr)
            output = p.stderr.decode().rstrip()
            print(p.stderr.decode().rstrip(), file=sys.stderr)
            print("-" * 70, file=sys.stderr)
        raise CommandFailedException(cmdline=cmdline, output=output)
    return p.stdout


def builds_to_delete(platform: str, to_delete: Set[str]) -> int:
    try:
        pkgs_json = run_command(
            [
                "conda",
                "search",
                "--json",
                "-c",
                "arrow-nightlies",
                "--override-channels",
                "--subdir",
                platform
            ],
        )
    except CommandFailedException as ex:
        # If the command failed due to no packages found, return
        # 0 builds to delete.
        if "PackagesNotFoundError" in ex.output:
            return 0
        else:
            sys.exit(1)

    pkgs = json.loads(pkgs_json)
    num_builds = 0

    for package_name, builds in pkgs.items():
        num_builds += len(builds)
        builds = pd.DataFrame(builds)
        builds["version"] = builds["version"].map(Version)
        # May be NaN if package doesn't depend on Python
        builds["py_version"] = builds["build"].str.extract(r'(py\d+)')
        builds["timestamp"] = pd.to_datetime(builds['timestamp'], unit='ms')
        builds["stale"] = builds["timestamp"] < DELETE_BEFORE
        # Some packages can be present in several "features" (e.g. CUDA),
        # others miss that column in which case we set a default value.
        if "track_features" not in builds.columns:
            if package_name == "arrow-cpp-proc":
                # XXX arrow-cpp-proc puts the features in the build field...
                builds["track_features"] = builds["build"]
            else:
                builds["track_features"] = 0

        # Detect old builds for each configuration:
        # a product of (architecture, Python version, features).
        for (subdir, python, features, stale), group in builds.groupby(
                ["subdir", "py_version", "track_features", "stale"],
                dropna=False):
            del_candidates = []
            if stale:
                del_candidates = group
            else:
                group = group.sort_values(by="version", ascending=False)
                if len(group) > VERSIONS_TO_KEEP:
                    del_candidates = group[VERSIONS_TO_KEEP:]

            if len(del_candidates):
                to_delete.update(
                    f"arrow-nightlies/{package_name}/"
                    + del_candidates["version"].astype(str)
                    + del_candidates["url"].str.replace(
                        "https://conda.anaconda.org/arrow-nightlies", "",
                        regex=False
                    )
                )

    return num_builds


if __name__ == "__main__":
    to_delete = set()
    num_builds = 0
    for platform in PLATFORMS:
        num_builds += builds_to_delete(platform, to_delete)

    to_delete = sorted(to_delete)

    print(f"{len(to_delete)} builds may be deleted out of {num_builds}")
    for name in to_delete:
        print(f"- {name}")

    if "FORCE" in sys.argv and len(to_delete) > 0:
        print("Deleting ...")
        run_command(["anaconda", "remove", "-f"] + to_delete)
