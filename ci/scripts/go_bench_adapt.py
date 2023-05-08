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

import json
import os
import uuid
import logging
from pathlib import Path
from typing import List, Optional, Dict

from benchadapt import BenchmarkResult
from benchadapt.adapters import BenchmarkAdapter
from benchadapt.log import log

log.setLevel(logging.DEBUG)

ARROW_ROOT = Path(__file__).parent.parent.parent.resolve()
SCRIPTS_PATH = ARROW_ROOT / "ci" / "scripts"

# `github_commit_info` is meant to communicate GitHub-flavored commit
# information to Conbench. See
# https://github.com/conbench/conbench/blob/7c4968e631ecdc064559c86a1174a1353713b700/benchadapt/python/benchadapt/result.py#L66
# for a specification.
github_commit_info: Optional[Dict] = None

if os.environ.get("CONBENCH_REF") == "main":
    # Assume GitHub Actions CI. The environment variable lookups below are
    # expected to fail when not running in GitHub Actions.
    github_commit_info = {
        "repository": os.environ["GITHUB_REPOSITORY"],
        "commit": os.environ["GITHUB_SHA"],
        "pr_number": None,  # implying default branch
    }
    run_reason = "commit"
else:
    # Assume that the environment is not GitHub Actions CI. Error out if that
    # assumption seems to be wrong.
    assert os.getenv("GITHUB_ACTIONS") is None

    # This is probably a local dev environment, for testing. In this case, it
    # does usually not make sense to provide commit information (not a
    # controlled CI environment). Explicitly keep `github_commit_info=None` to
    # reflect that (to not send commit information).

    # Reflect 'local dev' scenario in run_reason. Allow user to (optionally)
    # inject a custom piece of information into the run reason here, from
    # environment.
    run_reason = "localdev"
    custom_reason_suffix = os.getenv("CONBENCH_CUSTOM_RUN_REASON")
    if custom_reason_suffix is not None:
        run_reason += f" {custom_reason_suffix.strip()}"


class GoAdapter(BenchmarkAdapter):
    result_file = "bench_stats.json"
    command = ["bash", SCRIPTS_PATH / "go_bench.sh", ARROW_ROOT, "-json"]

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(command=self.command, *args, **kwargs)

    def _transform_results(self) -> List[BenchmarkResult]:
        with open(self.result_file, "r") as f:
            raw_results = json.load(f)

        run_id = uuid.uuid4().hex
        parsed_results = []
        for suite in raw_results[0]["Suites"]:
            batch_id = uuid.uuid4().hex
            pkg = suite["Pkg"]

            for benchmark in suite["Benchmarks"]:
                data = benchmark["Mem"]["MBPerSec"] * 1e6
                time = 1 / benchmark["NsPerOp"] * 1e9

                name = benchmark["Name"].removeprefix("Benchmark")
                ncpu = name[name.rfind("-") + 1 :]
                pieces = name[: -(len(ncpu) + 1)].split("/")

                parsed = BenchmarkResult(
                    run_id=run_id,
                    batch_id=batch_id,
                    stats={
                        "data": [data],
                        "unit": "b/s",
                        "times": [time],
                        "time_unit": "i/s",
                        "iterations": benchmark["Runs"],
                    },
                    context={
                        "benchmark_language": "Go",
                        "goos": suite["Goos"],
                        "goarch": suite["Goarch"],
                    },
                    tags={
                        "pkg": pkg,
                        "num_cpu": ncpu,
                        "name": pieces[0],
                        "params": "/".join(pieces[1:]),
                    },
                    run_reason=run_reason,
                    github=github_commit_info,
                )
                if github_commit_info is not None:
                    parsed.run_name = (
                        f"{parsed.run_reason}: {github_commit_info['commit']}"
                    )
                parsed_results.append(parsed)

        return parsed_results


if __name__ == "__main__":
    go_adapter = GoAdapter(result_fields_override={"info": {}})
    go_adapter()
