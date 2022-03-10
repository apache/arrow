from subprocess import check_output, check_call
from typing import List, Set

import json
import os
import pandas as pd
import sys

from packaging.version import Version


VERSIONS_TO_KEEP = 5
DELETE_BEFORE = pd.Timestamp.now() - pd.Timedelta(days=30)

PLATFORMS = [
    "linux-64",
    "linux-aarch64",
    "osx-64",
    "osx-arm64",
    "win-64",
]


def packages_to_delete(platform: str, to_delete: Set[str]):
    env = os.environ.copy()
    env["CONDA_SUBDIR"] = platform
    pkgs_json = check_output(
        [
            "conda",
            "search",
            "--json",
            "-c",
            "arrow-nightlies",
            "--override-channels",
        ],
        env=env,
    )
    pkgs = json.loads(pkgs_json)

    for package_name, builds in pkgs.items():
        builds = pd.DataFrame(builds)
        builds["version"] = builds["version"].map(Version)
        # May be NaN if package doesn't depend on Python
        builds["py_version"] = builds["build"].str.extract(r'(py\d+)')
        builds["timestamp"] = pd.to_datetime(builds['timestamp'], unit='ms')
        builds["stale"] = builds["timestamp"]< DELETE_BEFORE

        for (subdir, python, stale), group in builds.groupby(
                ["subdir", "py_version", "stale"]):
            if stale:
                del_candidates = group
            else:
                group = group.sort_values(by="version", ascending=False)
                if len(group) > VERSIONS_TO_KEEP:
                    del_candidates = group[VERSIONS_TO_KEEP:]

            to_delete.update(
                f"arrow-nightlies/{package_name}/"
                + del_candidates["version"].astype(str)
                + del_candidates["url"].str.replace(
                    "https://conda.anaconda.org/arrow-nightlies", "",
                    regex=False
                )
            )

    return to_delete


if __name__ == "__main__":
    to_delete = set()
    for platform in PLATFORMS:
        packages_to_delete(platform, to_delete)

    to_delete = sorted(to_delete)

    print(f"{len(to_delete)} packages may be deleted")
    for name in sorted(to_delete):
        print(f"Deleting {name} …")

    if "FORCE" in sys.argv:
        check_call(["anaconda", "remove", "-f"] + to_delete)
