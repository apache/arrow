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
import json
import os
import re

from .core import BenchmarkSuite
from .google import GoogleBenchmarkCommand, GoogleBenchmark
from .jmh import JavaMicrobenchmarkHarnessCommand, JavaMicrobenchmarkHarness
from ..lang.cpp import CppCMakeDefinition, CppConfiguration
from ..lang.java import JavaMavenDefinition, JavaConfiguration
from ..utils.cmake import CMakeBuild
from ..utils.maven import MavenBuild
from ..utils.logger import logger


def regex_filter(re_expr):
    if re_expr is None:
        return lambda s: True
    re_comp = re.compile(re_expr)
    return lambda s: re_comp.search(s)


DEFAULT_REPETITIONS = 1


class BenchmarkRunner:
    def __init__(self, suite_filter=None, benchmark_filter=None,
                 repetitions=DEFAULT_REPETITIONS, repetition_min_time=None):
        self.suite_filter = suite_filter
        self.benchmark_filter = benchmark_filter
        self.repetitions = repetitions
        self.repetition_min_time = repetition_min_time

    @property
    def suites(self):
        raise NotImplementedError("BenchmarkRunner must implement suites")

    @staticmethod
    def from_rev_or_path(src, root, rev_or_path, cmake_conf, **kwargs):
        raise NotImplementedError(
            "BenchmarkRunner must implement from_rev_or_path")


class StaticBenchmarkRunner(BenchmarkRunner):
    """ Run suites from a (static) set of suites. """

    def __init__(self, suites, **kwargs):
        self._suites = suites
        super().__init__(**kwargs)

    @property
    def list_benchmarks(self):
        for suite in self._suites:
            for benchmark in suite.benchmarks:
                yield "{}.{}".format(suite.name, benchmark.name)

    @property
    def suites(self):
        suite_fn = regex_filter(self.suite_filter)
        benchmark_fn = regex_filter(self.benchmark_filter)

        for suite in (s for s in self._suites if suite_fn(s.name)):
            benchmarks = [b for b in suite.benchmarks if benchmark_fn(b.name)]
            yield BenchmarkSuite(suite.name, benchmarks)

    @classmethod
    def is_json_result(cls, path_or_str):
        builder = None
        try:
            builder = cls.from_json(path_or_str)
        except BaseException:
            pass

        return builder is not None

    @staticmethod
    def from_json(path_or_str, **kwargs):
        # .codec imported here to break recursive imports
        from .codec import BenchmarkRunnerCodec
        if os.path.isfile(path_or_str):
            with open(path_or_str) as f:
                loaded = json.load(f)
        else:
            loaded = json.loads(path_or_str)
        return BenchmarkRunnerCodec.decode(loaded, **kwargs)

    def __repr__(self):
        return "BenchmarkRunner[suites={}]".format(list(self.suites))


class CppBenchmarkRunner(BenchmarkRunner):
    """ Run suites from a CMakeBuild. """

    def __init__(self, build, benchmark_extras, **kwargs):
        """ Initialize a CppBenchmarkRunner. """
        self.build = build
        self.benchmark_extras = benchmark_extras
        super().__init__(**kwargs)

    @staticmethod
    def default_configuration(**kwargs):
        """ Returns the default benchmark configuration. """
        return CppConfiguration(
            build_type="release", with_tests=False, with_benchmarks=True,
            with_compute=True,
            with_csv=True,
            with_dataset=True,
            with_json=True,
            with_jemalloc=True,
            with_mimalloc=True,
            with_parquet=True,
            with_python=False,
            with_brotli=True,
            with_bz2=True,
            with_lz4=True,
            with_snappy=True,
            with_zlib=True,
            with_zstd=True,
            **kwargs)

    @property
    def suites_binaries(self):
        """ Returns a list of benchmark binaries for this build. """
        # Ensure build is up-to-date to run benchmarks
        self.build()
        # Not the best method, but works for now
        glob_expr = os.path.join(self.build.binaries_dir, "*-benchmark")
        return {os.path.basename(b): b for b in glob.glob(glob_expr)}

    def suite(self, name, suite_bin):
        """ Returns the resulting benchmarks for a given suite. """
        suite_cmd = GoogleBenchmarkCommand(suite_bin, self.benchmark_filter,
                                           self.benchmark_extras)

        # Ensure there will be data
        benchmark_names = suite_cmd.list_benchmarks()
        if not benchmark_names:
            return None

        results = suite_cmd.results(
            repetitions=self.repetitions,
            repetition_min_time=self.repetition_min_time)
        benchmarks = GoogleBenchmark.from_json(results.get("benchmarks"))
        return BenchmarkSuite(name, benchmarks)

    @property
    def list_benchmarks(self):
        for suite_name, suite_bin in self.suites_binaries.items():
            suite_cmd = GoogleBenchmarkCommand(suite_bin)
            for benchmark_name in suite_cmd.list_benchmarks():
                yield "{}.{}".format(suite_name, benchmark_name)

    @property
    def suites(self):
        """ Returns all suite for a runner. """
        suite_matcher = regex_filter(self.suite_filter)

        suite_found = False
        suite_and_binaries = self.suites_binaries
        for suite_name in suite_and_binaries:
            if not suite_matcher(suite_name):
                logger.debug("Ignoring suite {}".format(suite_name))
                continue

            suite_bin = suite_and_binaries[suite_name]
            suite = self.suite(suite_name, suite_bin)

            # Filter may exclude all benchmarks
            if not suite:
                logger.debug("Suite {} executed but no results"
                             .format(suite_name))
                continue

            suite_found = True
            yield suite

        if not suite_found:
            raise ValueError("No benchmark matches the suite/benchmark filter")

    @staticmethod
    def from_rev_or_path(src, root, rev_or_path, cmake_conf, **kwargs):
        """ Returns a BenchmarkRunner from a path or a git revision.

        First, it checks if `rev_or_path` is a valid path (or string) of a json
        object that can deserialize to a BenchmarkRunner. If so, it initialize
        a StaticBenchmarkRunner from it. This allows memoizing the result of a
        run in a file or a string.

        Second, it checks if `rev_or_path` points to a valid CMake build
        directory.  If so, it creates a CppBenchmarkRunner with this existing
        CMakeBuild.

        Otherwise, it assumes `rev_or_path` is a revision and clone/checkout
        the given revision and create a fresh CMakeBuild.
        """
        build = None
        if StaticBenchmarkRunner.is_json_result(rev_or_path):
            kwargs.pop('benchmark_extras', None)
            return StaticBenchmarkRunner.from_json(rev_or_path, **kwargs)
        elif CMakeBuild.is_build_dir(rev_or_path):
            build = CMakeBuild.from_path(rev_or_path)
            return CppBenchmarkRunner(build, **kwargs)
        else:
            # Revisions can references remote via the `/` character, ensure
            # that the revision is path friendly
            path_rev = rev_or_path.replace("/", "_")
            root_rev = os.path.join(root, path_rev)
            os.mkdir(root_rev)

            clone_dir = os.path.join(root_rev, "arrow")
            # Possibly checkout the sources at given revision, no need to
            # perform cleanup on cloned repository as root_rev is reclaimed.
            src_rev, _ = src.at_revision(rev_or_path, clone_dir)
            cmake_def = CppCMakeDefinition(src_rev.cpp, cmake_conf)
            build_dir = os.path.join(root_rev, "build")
            return CppBenchmarkRunner(cmake_def.build(build_dir), **kwargs)


class JavaBenchmarkRunner(BenchmarkRunner):
    """ Run suites for Java. """

    # default repetitions is 5 for Java microbenchmark harness
    def __init__(self, build, **kwargs):
        """ Initialize a JavaBenchmarkRunner. """
        self.build = build
        super().__init__(**kwargs)

    @staticmethod
    def default_configuration(**kwargs):
        """ Returns the default benchmark configuration. """
        return JavaConfiguration(**kwargs)

    def suite(self, name):
        """ Returns the resulting benchmarks for a given suite. """
        # update .m2 directory, which installs target jars
        self.build.build()

        suite_cmd = JavaMicrobenchmarkHarnessCommand(
            self.build, self.benchmark_filter)

        # Ensure there will be data
        benchmark_names = suite_cmd.list_benchmarks()
        if not benchmark_names:
            return None

        # TODO: support `repetition_min_time`
        results = suite_cmd.results(repetitions=self.repetitions)
        benchmarks = JavaMicrobenchmarkHarness.from_json(results)
        return BenchmarkSuite(name, benchmarks)

    @property
    def list_benchmarks(self):
        """ Returns all suite names """
        # Ensure build is up-to-date to run benchmarks
        self.build.build()

        suite_cmd = JavaMicrobenchmarkHarnessCommand(self.build)
        benchmark_names = suite_cmd.list_benchmarks()
        for benchmark_name in benchmark_names:
            yield "{}".format(benchmark_name)

    @property
    def suites(self):
        """ Returns all suite for a runner. """
        suite_name = "JavaBenchmark"
        suite = self.suite(suite_name)

        # Filter may exclude all benchmarks
        if not suite:
            logger.debug("Suite {} executed but no results"
                         .format(suite_name))
            return

        yield suite

    @staticmethod
    def from_rev_or_path(src, root, rev_or_path, maven_conf, **kwargs):
        """ Returns a BenchmarkRunner from a path or a git revision.

        First, it checks if `rev_or_path` is a valid path (or string) of a json
        object that can deserialize to a BenchmarkRunner. If so, it initialize
        a StaticBenchmarkRunner from it. This allows memoizing the result of a
        run in a file or a string.

        Second, it checks if `rev_or_path` points to a valid Maven build
        directory.  If so, it creates a JavaBenchmarkRunner with this existing
        MavenBuild.

        Otherwise, it assumes `rev_or_path` is a revision and clone/checkout
        the given revision and create a fresh MavenBuild.
        """
        if StaticBenchmarkRunner.is_json_result(rev_or_path):
            return StaticBenchmarkRunner.from_json(rev_or_path, **kwargs)
        elif MavenBuild.is_build_dir(rev_or_path):
            maven_def = JavaMavenDefinition(rev_or_path, maven_conf)
            return JavaBenchmarkRunner(maven_def.build(rev_or_path), **kwargs)
        else:
            # Revisions can references remote via the `/` character, ensure
            # that the revision is path friendly
            path_rev = rev_or_path.replace("/", "_")
            root_rev = os.path.join(root, path_rev)
            os.mkdir(root_rev)

            clone_dir = os.path.join(root_rev, "arrow")
            # Possibly checkout the sources at given revision, no need to
            # perform cleanup on cloned repository as root_rev is reclaimed.
            src_rev, _ = src.at_revision(rev_or_path, clone_dir)
            maven_def = JavaMavenDefinition(src_rev.java, maven_conf)
            build_dir = os.path.join(root_rev, "arrow/java")
            return JavaBenchmarkRunner(maven_def.build(build_dir), **kwargs)
