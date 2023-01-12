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
from typing import List

from benchadapt import BenchmarkResult
from benchadapt.adapters import BenchmarkAdapter
from benchadapt.log import log

log.setLevel(logging.DEBUG)

ARROW_ROOT = Path(__file__).parent.parent.parent.resolve()
SCRIPTS_PATH = ARROW_ROOT / "ci" / "scripts"

if os.environ.get("CONBENCH_REF") == "master":
    github = {
        "repository": os.environ["GITHUB_REPOSITORY"],
        "commit": os.environ["GITHUB_SHA"],
        "pr_number": None,  # implying default branch
    }
    run_reason = "commit"
else:
    github = None  # scrape github info from the local repo
    run_reason = "branch"

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

                name = benchmark["Name"].removeprefix('Benchmark')
                ncpu = name[name.rfind('-')+1:]
                pieces = name[:-(len(ncpu)+1)].split('/')

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
                        "params": '/'.join(pieces[1:]),
                    },
                    run_reason=run_reason,
                    github=github,
                )
                parsed.run_name = f"{parsed.run_reason}: {parsed.github['commit']}"
                parsed_results.append(parsed)

        return parsed_results


if __name__ == "__main__":
    go_adapter = GoAdapter(result_fields_override={"info":{}})
    go_adapter()
