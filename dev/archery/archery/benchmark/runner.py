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

from .core import BenchmarkSuite
from .google import GoogleBenchmarkCommand, GoogleBenchmark
from ..lang.cpp import CppCMakeDefinition
from ..utils.cmake import CMakeBuild
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

    @property
    def suites_binaries(self):
        """ Returns a list of benchmark binaries for this build. """
        # Ensure build is up-to-date to run benchmarks
        self.build()
        # Not the best method, but works for now
        glob_expr = os.path.join(self.build.binaries_dir, "*-benchmark")
        return {os.path.basename(b): b for b in glob.glob(glob_expr)}

    def suite(self, name, suite_bin, benchmark_filter):
        """ Returns the resulting benchmarks for a given suite. """
        suite_cmd = GoogleBenchmarkCommand(suite_bin, benchmark_filter)

        # Ensure there will be data
        benchmark_names = suite_cmd.list_benchmarks()
        if not benchmark_names:
            return None

        results = suite_cmd.results()
        benchmarks = GoogleBenchmark.from_json(results.get("benchmarks"))
        return BenchmarkSuite(name, benchmarks)

    def suites(self, suite_filter=None, benchmark_filter=None):
        """ Returns all suite for a runner. """
        suite_matcher = regex_filter(suite_filter)

        suite_and_binaries = self.suites_binaries
        for suite_name in suite_and_binaries:
            if not suite_matcher(suite_name):
                logger.debug(f"Ignoring suite {suite_name}")
                continue

            suite_bin = suite_and_binaries[suite_name]
            suite = self.suite(suite_name, suite_bin,
                               benchmark_filter=benchmark_filter)

            # Filter may exclude all benchmarks
            if not suite:
                logger.debug(f"Suite {suite_name} executed but no results")
                continue

            yield suite

    @staticmethod
    def from_rev_or_path(src, root, rev_or_path, cmake_conf):
        """ Returns a CppBenchmarkRunner from a path or a git revision.

        First, it checks if `rev_or_path` points to a valid CMake build
        directory.  If so, it creates a CppBenchmarkRunner with this existing
        CMakeBuild.

        Otherwise, it assumes `rev_or_path` is a revision and clone/checkout
        the given revision and create a fresh CMakeBuild.
        """
        build = None
        if CMakeBuild.is_build_dir(rev_or_path):
            build = CMakeBuild.from_path(rev_or_path)
        else:
            root_rev = os.path.join(root, rev_or_path)
            os.mkdir(root_rev)

            clone_dir = os.path.join(root_rev, "arrow")
            # Possibly checkout the sources at given revision, no need to
            # perform cleanup on cloned repository as root_rev is reclaimed.
            src_rev, _ = src.at_revision(rev_or_path, clone_dir)
            cmake_def = CppCMakeDefinition(src_rev.cpp, cmake_conf)
            build = cmake_def.build(os.path.join(root_rev, "build"))

        return CppBenchmarkRunner(build)
