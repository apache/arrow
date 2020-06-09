from distutils.version import LooseVersion
from subprocess import check_output, check_call
from typing import List

import json
import os
import pandas as pd
import sys


VERSIONS_TO_KEEP = 5
PLATFORMS = ["linux-64", "osx-64", "win-64"]
PACKAGES = ["pyarrow", "arrow-cpp"]


def packages_to_delete(package_name: str, platform: str) -> List[str]:
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
            package_name,
        ],
        env=env,
    )
    pkgs = pd.DataFrame(json.loads(pkgs_json)[package_name])
    pkgs["version"] = pkgs["version"].map(LooseVersion)
    pkgs["py_version"] = pkgs["build"].str.slice(0, 4)

    to_delete = []

    for (subdir, python), group in pkgs.groupby(["subdir", "py_version"]):
        group = group.sort_values(by="version", ascending=False)

        if len(group) > VERSIONS_TO_KEEP:
            del_candidates = group[VERSIONS_TO_KEEP:]
            to_delete += (
                f"arrow-nightlies/{package_name}/"
                + del_candidates["version"].astype(str)
                + del_candidates["url"].str.replace(
                    "https://conda.anaconda.org/arrow-nightlies", ""
                )
            ).to_list()

    return to_delete


if __name__ == "__main__":
    to_delete = []
    for platform in PLATFORMS:
        for package in PACKAGES:
            to_delete += packages_to_delete(package, platform)

    for name in to_delete:
        print(f"Deleting {name} …")
        if "FORCE" in sys.argv:
            check_call(["anaconda", "remove", "-f", name])
