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

import click
from contextlib import contextmanager
import json
import logging
import sys
from tempfile import mkdtemp, TemporaryDirectory

from .benchmark.compare import RunnerComparator, DEFAULT_THRESHOLD
from .benchmark.runner import BenchmarkRunner
from .lang.cpp import CppCMakeDefinition, CppConfiguration
from .utils.codec import JsonEncoder
from .utils.logger import logger, ctx as log_ctx
from .utils.source import ArrowSources

# Set default logging to INFO in command line.
logging.basicConfig(level=logging.INFO)


@click.group()
@click.option("--debug", type=bool, is_flag=True, default=False,
              help="Increase logging with debugging output.")
@click.option("-q", "--quiet", type=bool, is_flag=True, default=False,
              help="Silence executed commands.")
@click.pass_context
def archery(ctx, debug, quiet):
    """ Apache Arrow developer utilities.

    See sub-commands help with `archery <cmd> --help`.

    """
    # Ensure ctx.obj exists
    ctx.ensure_object(dict)

    log_ctx.quiet = quiet
    if debug:
        logger.setLevel(logging.DEBUG)


def validate_arrow_sources(ctx, param, src):
    """ Ensure a directory contains Arrow cpp sources. """
    if isinstance(src, str):
        if not ArrowSources.valid(src):
            raise click.BadParameter(f"No Arrow C++ sources found in {src}.")
        src = ArrowSources(src)
    return src


build_dir_type = click.Path(dir_okay=True, file_okay=False, resolve_path=True)
# Supported build types
build_type = click.Choice(["debug", "relwithdebinfo", "release"],
                          case_sensitive=False)
# Supported warn levels
warn_level_type = click.Choice(["everything", "checkin", "production"],
                               case_sensitive=False)


@archery.command(short_help="Initialize an Arrow C++ build")
@click.option("--src", metavar="<arrow_src>", default=ArrowSources.find(),
              callback=validate_arrow_sources,
              help="Specify Arrow source directory")
# toolchain
@click.option("--cc", metavar="<compiler>", help="C compiler.")
@click.option("--cxx", metavar="<compiler>", help="C++ compiler.")
@click.option("--cxx-flags", help="C++ compiler flags.")
@click.option("--build-type", default="release", type=build_type,
              help="CMake's CMAKE_BUILD_TYPE")
@click.option("--warn-level", default="production", type=warn_level_type,
              help="Controls compiler warnings -W(no-)error.")
# components
@click.option("--with-tests", default=True, type=bool,
              help="Build with tests.")
@click.option("--with-benchmarks", default=False, type=bool,
              help="Build with benchmarks.")
@click.option("--with-python", default=True, type=bool,
              help="Build with python extension.")
@click.option("--with-parquet", default=False, type=bool,
              help="Build with parquet file support.")
@click.option("--with-gandiva", default=False, type=bool,
              help="Build with Gandiva expression compiler support.")
@click.option("--with-plasma", default=False, type=bool,
              help="Build with Plasma object store support.")
@click.option("--with-flight", default=False, type=bool,
              help="Build with Flight rpc support.")
@click.option("--cmake-extras", type=str, multiple=True,
              help="Extra flags/options to pass to cmake invocation. "
              "Can be stacked")
# misc
@click.option("-f", "--force", type=bool, is_flag=True, default=False,
              help="Delete existing build directory if found.")
@click.option("--targets", type=str, multiple=True,
              help="Generator targets to run. Can be stacked.")
@click.argument("build_dir", type=build_dir_type)
@click.pass_context
def build(ctx, src, build_dir, force, targets, **kwargs):
    """ Initialize a C++ build directory.

    The build command creates a directory initialized with Arrow's cpp source
    cmake and configuration. It can also optionally invoke the generator to
    test the build (and used in scripts).

    Note that archery will carry the caller environment. It will also not touch
    an existing directory, one must use the `--force` option to remove the
    existing directory.

    Examples:

    \b
    # Initialize build with clang7 and avx2 support in directory `clang7-build`
    \b
    archery build --cc=clang-7 --cxx=clang++-7 --cxx-flags=-mavx2 clang7-build

    \b
    # Builds and run test
    archery build --targets=all --targets=test build
    """
    # Arrow's cpp cmake configuration
    conf = CppConfiguration(**kwargs)
    # This is a closure around cmake invocation, e.g. calling `def.build()`
    # yields a directory ready to be run with the generator
    cmake_def = CppCMakeDefinition(src.cpp, conf)
    # Create build directory
    build = cmake_def.build(build_dir, force=force)

    for target in targets:
        build.run(target)


@contextmanager
def tmpdir(preserve, prefix="arrow-bench-"):
    if preserve:
        yield mkdtemp(prefix=prefix)
    else:
        with TemporaryDirectory(prefix=prefix) as tmp:
            yield tmp


# Running all benchmarks would be prohibitive. Benchmark who needs to be
# monitored for regression should be named with this prefix.
DEFAULT_BENCHMARK_FILTER = "^Regression"


@archery.group()
@click.pass_context
def benchmark(ctx):
    """ Arrow benchmarking.

    Use the diff sub-command to benchmake revisions, and/or build directories.
    """
    pass


@benchmark.command(name="run", short_help="Run benchmark suite")
@click.option("--src", metavar="<arrow_src>", show_default=True,
              default=ArrowSources.find(),
              callback=validate_arrow_sources,
              help="Specify Arrow source directory")
@click.option("--suite-filter", metavar="<regex>", show_default=True,
              type=str, default=None, help="Regex filtering benchmark suites.")
@click.option("--benchmark-filter", metavar="<regex>", show_default=True,
              type=str, default=DEFAULT_BENCHMARK_FILTER,
              help="Regex filtering benchmark suites.")
@click.option("--preserve", type=bool, default=False, show_default=True,
              is_flag=True, help="Preserve workspace for investigation.")
@click.option("--output", metavar="<output>",
              type=click.File("w", encoding="utf8"), default="-",
              help="Capture output result into file.")
@click.option("--cmake-extras", type=str, multiple=True,
              help="Extra flags/options to pass to cmake invocation. "
              "Can be stacked")
@click.argument("baseline", metavar="[<baseline>]]", default="master",
                required=False)
@click.pass_context
def benchmark_run(ctx, src, preserve, suite_filter, benchmark_filter,
                  output, cmake_extras, baseline):
    """ Run benchmark suite.

    This command will run the benchmark suite for a single build. This is
    used to capture (and/or publish) the results.

    The caller can optionally specify a target which is either a git revision
    (commit, tag, special values like HEAD) or a cmake build directory.


    When a commit is referenced, a local clone of the arrow sources (specified
    via --src) is performed and the proper branch is created. This is done in
    a temporary directory which can be left intact with the `---preserve` flag.

    The special token "WORKSPACE" is reserved to specify the current git
    workspace. This imply that no clone will be performed.

    Examples:

    \b
    # Run the benchmarks on current git workspace
    \b
    archery benchmark run

    \b
    # Run the benchmarks on current previous commit
    \b
    archery benchmark run HEAD~1

    \b
    # Run the benchmarks on current previous commit
    \b
    archery benchmark run --output=run.json
    """
    with tmpdir(preserve) as root:
        logger.debug(f"Running benchmark {baseline}")

        conf = CppConfiguration(
            build_type="release", with_tests=True, with_benchmarks=True,
            with_python=False, cmake_extras=cmake_extras)

        runner_base = BenchmarkRunner.from_rev_or_path(
            src, root, baseline, conf,
            suite_filter=suite_filter, benchmark_filter=benchmark_filter)

        json.dump(runner_base, output, cls=JsonEncoder)


@benchmark.command(name="diff", short_help="Compare benchmark suites")
@click.option("--src", metavar="<arrow_src>", show_default=True,
              default=ArrowSources.find(),
              callback=validate_arrow_sources,
              help="Specify Arrow source directory")
@click.option("--suite-filter", metavar="<regex>", show_default=True,
              type=str, default=None, help="Regex filtering benchmark suites.")
@click.option("--benchmark-filter", metavar="<regex>", show_default=True,
              type=str, default=DEFAULT_BENCHMARK_FILTER,
              help="Regex filtering benchmark suites.")
@click.option("--preserve", type=bool, default=False, show_default=True,
              is_flag=True, help="Preserve workspace for investigation.")
@click.option("--threshold", type=float, default=DEFAULT_THRESHOLD,
              show_default=True,
              help="Regression failure threshold in percentage.")
@click.option("--output", metavar="<output>",
              type=click.File("w", encoding="utf8"), default="-",
              help="Capture output result into file.")
@click.option("--cmake-extras", type=str, multiple=True,
              help="Extra flags/options to pass to cmake invocation. "
              "Can be stacked")
@click.argument("contender", metavar="[<contender>",
                default=ArrowSources.WORKSPACE, required=False)
@click.argument("baseline", metavar="[<baseline>]]", default="master",
                required=False)
@click.pass_context
def benchmark_diff(ctx, src, preserve, suite_filter, benchmark_filter,
                   threshold, output, cmake_extras, contender, baseline):
    """ Compare (diff) benchmark runs.

    This command acts like git-diff but for benchmark results.

    The caller can optionally specify both the contender and the baseline. If
    unspecified, the contender will default to the current workspace (like git)
    and the baseline will default to master.

    Each target (contender or baseline) can either be a git revision
    (commit, tag, special values like HEAD) or a cmake build directory. This
    allow comparing git commits, and/or different compilers and/or compiler
    flags.

    When a commit is referenced, a local clone of the arrow sources (specified
    via --src) is performed and the proper branch is created. This is done in
    a temporary directory which can be left intact with the `---preserve` flag.

    The special token "WORKSPACE" is reserved to specify the current git
    workspace. This imply that no clone will be performed.

    Examples:

    \b
    # Compare workspace (contender) with master (baseline)
    \b
    archery benchmark diff

    \b
    # Compare master (contender) with latest version (baseline)
    \b
    export LAST=$(git tag -l "apache-arrow-[0-9]*" | sort -rV | head -1)
    \b
    archery benchmark diff master "$LAST"

    \b
    # Compare g++7 (contender) with clang++-7 (baseline) builds
    \b
    archery build --with-benchmarks=true \\
            --cxx-flags=-ftree-vectorize \\
            --cc=gcc-7 --cxx=g++-7 gcc7-build
    \b
    archery build --with-benchmarks=true \\
            --cxx-flags=-flax-vector-conversions \\
            --cc=clang-7 --cxx=clang++-7 clang7-build
    \b
    archery benchmark diff gcc7-build clang7-build

    \b
    # Compare default targets but scoped to the suites matching
    # `^arrow-compute-aggregate` and benchmarks matching `(Sum|Mean)Kernel`.
    \b
    archery benchmark diff --suite-filter="^arrow-compute-aggregate" \\
            --benchmark-filter="(Sum|Mean)Kernel"

    \b
    # Capture result in file `result.json`
    \b
    archery benchmark diff --output=result.json
    \b
    # Equivalently with no stdout clutter.
    archery --quiet benchmark diff > result.json

    \b
    # Comparing with a cached results from `archery benchmark run`
    \b
    archery benchmark run --output=run.json HEAD~1
    \b
    # This should not recompute the benchmark from run.json
    archery --quiet benchmark diff WORKSPACE run.json > result.json
    """
    with tmpdir(preserve) as root:
        logger.debug(f"Comparing {contender} (contender) with "
                     f"{baseline} (baseline)")

        conf = CppConfiguration(
            build_type="release", with_tests=True, with_benchmarks=True,
            with_python=False, cmake_extras=cmake_extras)

        runner_cont = BenchmarkRunner.from_rev_or_path(
            src, root, contender, conf,
            suite_filter=suite_filter, benchmark_filter=benchmark_filter)
        runner_base = BenchmarkRunner.from_rev_or_path(
            src, root, baseline, conf,
            suite_filter=suite_filter, benchmark_filter=benchmark_filter)

        regressions = 0
        runner_comp = RunnerComparator(runner_cont, runner_base, threshold)
        for comparator in runner_comp.comparisons:
            regressions += comparator.regression
            json.dump(comparator, output, cls=JsonEncoder)

        sys.exit(regressions)


if __name__ == "__main__":
    archery(obj={})
