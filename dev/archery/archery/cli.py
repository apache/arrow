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
import errno
import json
import logging
import os
import sys

from .benchmark.compare import RunnerComparator, DEFAULT_THRESHOLD
from .benchmark.runner import BenchmarkRunner
from .lang.cpp import CppCMakeDefinition, CppConfiguration
from .utils.codec import JsonEncoder
from .utils.lint import linter, LintValidationException
from .utils.logger import logger, ctx as log_ctx
from .utils.source import ArrowSources
from .utils.tmpdir import tmpdir

# Set default logging to INFO in command line.
logging.basicConfig(level=logging.INFO)


class ArrowBool(click.types.BoolParamType):
    """
    ArrowBool supports the 'ON' and 'OFF' values on top of the values
    supported by BoolParamType. This is convenient to port script which exports
    CMake options variables.
    """
    name = "boolean"

    def convert(self, value, param, ctx):
        if isinstance(value, str):
            lowered = value.lower()
            if lowered == "on":
                return True
            elif lowered == "off":
                return False

        return super().convert(value, param, ctx)


BOOL = ArrowBool()


@click.group()
@click.option("--debug", type=BOOL, is_flag=True, default=False,
              help="Increase logging with debugging output.")
@click.option("--pdb", type=BOOL, is_flag=True, default=False,
              help="Invoke pdb on uncaught exception.")
@click.option("-q", "--quiet", type=BOOL, is_flag=True, default=False,
              help="Silence executed commands.")
@click.pass_context
def archery(ctx, debug, pdb, quiet):
    """ Apache Arrow developer utilities.

    See sub-commands help with `archery <cmd> --help`.

    """
    # Ensure ctx.obj exists
    ctx.ensure_object(dict)

    log_ctx.quiet = quiet
    if debug:
        logger.setLevel(logging.DEBUG)

    ctx.debug = debug

    if pdb:
        import pdb
        sys.excepthook = lambda t, v, e: pdb.pm()


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


def cpp_toolchain_options(cmd):
    options = [
        click.option("--cc", metavar="<compiler>", help="C compiler."),
        click.option("--cxx", metavar="<compiler>", help="C++ compiler."),
        click.option("--cxx-flags", help="C++ compiler flags."),
        click.option("--cpp-package-prefix",
                     help=("Value to pass for ARROW_PACKAGE_PREFIX and "
                           "use ARROW_DEPENDENCY_SOURCE=SYSTEM"))
    ]
    return _apply_options(cmd, options)


def _apply_options(cmd, options):
    for option in options:
        cmd = option(cmd)
    return cmd


@archery.command(short_help="Initialize an Arrow C++ build")
@click.option("--src", metavar="<arrow_src>", default=ArrowSources.find(),
              callback=validate_arrow_sources,
              help="Specify Arrow source directory")
# toolchain
@cpp_toolchain_options
@click.option("--build-type", default="release", type=build_type,
              help="CMake's CMAKE_BUILD_TYPE")
@click.option("--warn-level", default="production", type=warn_level_type,
              help="Controls compiler warnings -W(no-)error.")
# components
@click.option("--with-tests", default=True, type=BOOL,
              help="Build with tests.")
@click.option("--with-benchmarks", default=False, type=BOOL,
              help="Build with benchmarks.")
@click.option("--with-python", default=True, type=BOOL,
              help="Build with python extension.")
@click.option("--with-parquet", default=False, type=BOOL,
              help="Build with parquet file support.")
@click.option("--with-gandiva", default=False, type=BOOL,
              help="Build with Gandiva expression compiler support.")
@click.option("--with-plasma", default=False, type=BOOL,
              help="Build with Plasma object store support.")
@click.option("--with-flight", default=False, type=BOOL,
              help="Build with Flight rpc support.")
@click.option("--cmake-extras", type=str, multiple=True,
              help="Extra flags/options to pass to cmake invocation. "
              "Can be stacked")
# misc
@click.option("-f", "--force", type=BOOL, is_flag=True, default=False,
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


@archery.command(short_help="Lint Arrow source directory")
@click.option("--src", metavar="<arrow_src>", default=ArrowSources.find(),
              callback=validate_arrow_sources,
              help="Specify Arrow source directory")
@click.option("--with-clang-format", default=True, type=BOOL,
              help="Ensure formatting of C++ files.")
@click.option("--with-cpplint", default=True, type=BOOL,
              help="Ensure linting of C++ files with cpplint.")
@click.option("--with-clang-tidy", default=False, type=BOOL,
              help="Lint C++ with clang-tidy.")
@click.option("--with-iwyu", default=False, type=BOOL,
              help="Lint C++ with Include-What-You-Use (iwyu).")
@click.option("--with-flake8", default=True, type=BOOL,
              help="Lint python files with flake8.")
@click.option("--with-cmake-format", default=True, type=BOOL,
              help="Lint CMakeFiles.txt files with cmake-format.py.")
@click.option("--with-rat", default=True, type=BOOL,
              help="Lint files for license violation via apache-rat.")
@click.option("--with-r", default=True, type=BOOL,
              help="Lint r files.")
@click.option("--with-rust", default=True, type=BOOL,
              help="Lint rust files.")
@click.option("--with-docker", default=True, type=BOOL,
              help="Lint docker images with hadolint.")
@click.option("--fix", type=BOOL, default=False,
              help="Toggle fixing the lint errors if the linter supports it.")
@click.pass_context
def lint(ctx, src, **kwargs):
    try:
        linter(src, **kwargs)
    except LintValidationException:
        sys.exit(1)


@archery.group()
@click.pass_context
def benchmark(ctx):
    """ Arrow benchmarking.

    Use the diff sub-command to benchmake revisions, and/or build directories.
    """
    pass


def benchmark_common_options(cmd):
    options = [
        click.option("--src", metavar="<arrow_src>", show_default=True,
                     default=ArrowSources.find(),
                     callback=validate_arrow_sources,
                     help="Specify Arrow source directory"),
        click.option("--preserve", type=BOOL, default=False, show_default=True,
                     is_flag=True,
                     help="Preserve workspace for investigation."),
        click.option("--output", metavar="<output>",
                     type=click.File("w", encoding="utf8"), default="-",
                     help="Capture output result into file."),
        click.option("--cmake-extras", type=str, multiple=True,
                     help="Extra flags/options to pass to cmake invocation. "
                     "Can be stacked"),
    ]

    cmd = cpp_toolchain_options(cmd)
    return _apply_options(cmd, options)


def benchmark_filter_options(cmd):
    options = [
        click.option("--suite-filter", metavar="<regex>", show_default=True,
                     type=str, default=None,
                     help="Regex filtering benchmark suites."),
        click.option("--benchmark-filter", metavar="<regex>",
                     show_default=True, type=str, default=None,
                     help="Regex filtering benchmark suites.")
    ]
    return _apply_options(cmd, options)


@benchmark.command(name="list", short_help="List benchmark suite")
@click.argument("rev_or_path", metavar="[<rev_or_path>]",
                default="WORKSPACE", required=False)
@benchmark_common_options
@click.pass_context
def benchmark_list(ctx, rev_or_path, src, preserve, output, cmake_extras,
                   **kwargs):
    """ List benchmark suite.
    """
    with tmpdir(preserve=preserve) as root:
        logger.debug(f"Running benchmark {rev_or_path}")

        conf = CppConfiguration(
            build_type="release", with_tests=True, with_benchmarks=True,
            with_python=False, cmake_extras=cmake_extras,
            **kwargs)

        runner_base = BenchmarkRunner.from_rev_or_path(
            src, root, rev_or_path, conf)

        for b in runner_base.list_benchmarks:
            click.echo(b, file=output)


@benchmark.command(name="run", short_help="Run benchmark suite")
@click.argument("rev_or_path", metavar="[<rev_or_path>]",
                default="WORKSPACE", required=False)
@benchmark_common_options
@benchmark_filter_options
@click.pass_context
def benchmark_run(ctx, rev_or_path, src, preserve, output, cmake_extras,
                  suite_filter, benchmark_filter, **kwargs):
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
    with tmpdir(preserve=preserve) as root:
        logger.debug(f"Running benchmark {rev_or_path}")

        conf = CppConfiguration(
            build_type="release", with_tests=True, with_benchmarks=True,
            with_python=False, cmake_extras=cmake_extras, **kwargs)

        runner_base = BenchmarkRunner.from_rev_or_path(
            src, root, rev_or_path, conf,
            suite_filter=suite_filter, benchmark_filter=benchmark_filter)

        json.dump(runner_base, output, cls=JsonEncoder)


@benchmark.command(name="diff", short_help="Compare benchmark suites")
@benchmark_common_options
@benchmark_filter_options
@click.option("--threshold", type=float, default=DEFAULT_THRESHOLD,
              show_default=True,
              help="Regression failure threshold in percentage.")
@click.argument("contender", metavar="[<contender>",
                default=ArrowSources.WORKSPACE, required=False)
@click.argument("baseline", metavar="[<baseline>]]", default="master",
                required=False)
@click.pass_context
def benchmark_diff(ctx, src, preserve, output, cmake_extras,
                   suite_filter, benchmark_filter,
                   threshold, contender, baseline, **kwargs):
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
    with tmpdir(preserve=preserve) as root:
        logger.debug(f"Comparing {contender} (contender) with "
                     f"{baseline} (baseline)")

        conf = CppConfiguration(
            build_type="release", with_tests=True, with_benchmarks=True,
            with_python=False, cmake_extras=cmake_extras, **kwargs)

        runner_cont = BenchmarkRunner.from_rev_or_path(
            src, root, contender, conf,
            suite_filter=suite_filter, benchmark_filter=benchmark_filter)
        runner_base = BenchmarkRunner.from_rev_or_path(
            src, root, baseline, conf,
            suite_filter=suite_filter, benchmark_filter=benchmark_filter)

        runner_comp = RunnerComparator(runner_cont, runner_base, threshold)

        # TODO(kszucs): test that the output is properly formatted jsonlines
        for comparator in runner_comp.comparisons:
            json.dump(comparator, output, cls=JsonEncoder)
            output.write("\n")


# ----------------------------------------------------------------------
# Integration testing

def _set_default(opt, default):
    if opt is None:
        return default
    return opt


@archery.command(short_help="Execute protocol and Flight integration tests")
@click.option('--with-all', is_flag=True, default=False,
              help=('Include all known lanauges by default '
                    ' in integration tests'))
@click.option('--random-seed', type=int, default=12345,
              help="Seed for PRNG when generating test data")
@click.option('--with-cpp', type=bool, default=False,
              help='Include C++ in integration tests')
@click.option('--with-java', type=bool, default=False,
              help='Include Java in integration tests')
@click.option('--with-js', type=bool, default=False,
              help='Include JavaScript in integration tests')
@click.option('--with-go', type=bool, default=False,
              help='Include Go in integration tests')
@click.option('--write_generated_json', default=False,
              help='Generate test JSON to indicated path')
@click.option('--run-flight', is_flag=True, default=False,
              help='Run Flight integration tests')
@click.option('--debug', type=bool, default=False,
              help='Run executables in debug mode as relevant')
@click.option('--tempdir', default=None,
              help=('Directory to use for writing '
                    'integration test temporary files'))
@click.option('stop_on_error', '-x', '--stop-on-error',
              is_flag=True, default=False,
              help='Stop on first error')
@click.option('--gold_dirs', multiple=True,
              help="gold integration test file paths")
def integration(with_all=False, random_seed=12345, **args):
    from .integration.runner import write_js_test_json, run_all_tests
    import numpy as np

    # Make runs involving data generation deterministic
    np.random.seed(random_seed)

    gen_path = args['write_generated_json']

    languages = ['cpp', 'java', 'js', 'go']

    enabled_languages = 0
    for lang in languages:
        param = 'with_{}'.format(lang)
        if with_all:
            args[param] = with_all

        if args[param]:
            enabled_languages += 1

    if gen_path:
        try:
            os.makedirs(gen_path)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise
        write_js_test_json(gen_path)
    else:
        if enabled_languages == 0:
            raise Exception("Must enable at least 1 language to test")
        run_all_tests(**args)


if __name__ == "__main__":
    archery(obj={})
