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

import glob
import os
import re
import subprocess

from .core import Benchmark, BenchmarkSuite
from .google import GoogleBenchmarkCommand
from ..utils.logger import logger


def regex_filter(re_expr):
    if re_expr is None:
        return lambda s: True

    re_comp = re.compile(re_expr)
    return lambda s: re_comp.search(s)


class BenchmarkRunner:
    def suites(self, suite_filter=None, benchmark_filter=None):
        raise NotImplementedError("BenchmarkRunner must implement suites")


class CppBenchmarkRunner(BenchmarkRunner):
    def __init__(self, build):
        """ Initialize a CppBenchmarkRunner. """
        self.build = build
        # Ensure build is ready to run benchmarks
        self.build.all()

    @property
    def suites_binaries(self):
        """ Returns a list of benchmark binaries for this build. """
        glob_expr = os.path.join(self.build.binaries_dir, "*-benchmark")
        return {os.path.basename(b): b for b in glob.glob(glob_expr)}

    def benchmarks(self, suite_bin, benchmark_filter):
        """ Returns the resulting benchmarks for a given suite. """
        suite_cmd = GoogleBenchmarkCommand(suite_bin)
        benchmark_names = suite_cmd.list_benchmarks(benchmark_filter)

        if not benchmark_names:
            return []

        return benchmark_names

    def suites(self, suite_filter=None, benchmark_filter=None):
        suite_matcher = regex_filter(suite_filter)

        suite_and_binaries = self.suites_binaries
        for suite_name in suite_and_binaries:
            if suite_matcher(suite_name):
                suite_bin = suite_and_binaries[suite_name]
                benchmarks = self.benchmarks(suite_bin,
                                             benchmark_filter=benchmark_filter)

                # Filter may exclude all benchmarks
                if not benchmarks:
                    continue

                yield BenchmarkSuite(suite_name, benchmarks)
            else:
                logger.debug(f"Ignoring suite {suite_name}")
