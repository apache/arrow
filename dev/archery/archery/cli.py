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
import logging
import os
import re
from tempfile import mkdtemp, TemporaryDirectory

# from .benchmark import GoogleCompareBenchmark
from .benchmark.runner import CppBenchmarkRunner
from .lang.cpp import CppCMakeDefinition, CppConfiguration
from .utils.git import Git
from .utils.logger import logger
from .utils.source import ArrowSources


@click.group()
@click.option("--debug", count=True)
def archery(debug):
    """ Apache Arrow developer utilities. """
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
@click.option("--src", default=ArrowSources.find(),
              callback=validate_arrow_sources,
              help="Specify Arrow source directory")
# toolchain
@click.option("--cc", help="C compiler.")
@click.option("--cxx", help="C++ compiler.")
@click.option("--cxx_flags", help="C++ compiler flags.")
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
# misc
@click.option("-f", "--force", type=bool, is_flag=True, default=False,
              help="Delete existing build directory if found.")
@click.option("--targets", type=str, multiple=True,
              help="Generator targets to run")
@click.argument("build_dir", type=build_dir_type)
def build(src, build_dir, force, targets, **kwargs):
    """ Build. """
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


DEFAULT_BENCHMARK_CONF = CppConfiguration(
    build_type="release", with_tests=True, with_benchmarks=True)


def arrow_cpp_benchmark_runner(src, root, rev):
    root_rev = os.path.join(root, rev)
    os.mkdir(root_rev)

    root_src = os.path.join(root_rev, "arrow")
    # Possibly checkout the sources at given revision
    src_rev = src if rev == Git.WORKSPACE else src.at_revision(rev, root_src)

    # TODO: find a way to pass custom configuration without cluttering the cli
    # Ideally via a configuration file that can be shared with the
    # `build` sub-command.
    cmake_def = CppCMakeDefinition(src_rev.cpp, DEFAULT_BENCHMARK_CONF)
    build = cmake_def.build(os.path.join(root_rev, "build"))
    return CppBenchmarkRunner(build)


DEFAULT_BENCHMARK_FILTER = "^Regression"


@archery.command(short_help="Run the C++ benchmark suite")
@click.option("--src", default=ArrowSources.find(),
              callback=validate_arrow_sources,
              help="Specify Arrow source directory")
@click.option("--preserve", type=bool, default=False, is_flag=True,
              help="Preserve temporary workspace directory for investigation.")
@click.option("--suite-filter", type=str, default=None,
              help="Regex filtering benchmark suites.")
@click.option("--benchmark-filter", type=str, default=DEFAULT_BENCHMARK_FILTER,
              help="Regex filtering benchmark suites.")
@click.argument("contender_rev", default=Git.WORKSPACE, required=False)
@click.argument("baseline_rev", default="master", required=False)
def benchmark(src, preserve, suite_filter,  benchmark_filter,
              contender_rev, baseline_rev):
    """ Benchmark Arrow C++ implementation.
    """
    with tmpdir(preserve) as root:
        runner_cont = arrow_cpp_benchmark_runner(src, root, contender_rev)
        runner_base = arrow_cpp_benchmark_runner(src, root, baseline_rev)

        suites_cont = {s.name: s for s in runner_cont.suites(suite_filter,
                                                             benchmark_filter)}
        suites_base = {s.name: s for s in runner_base.suites(suite_filter,
                                                             benchmark_filter)}

        for name in suites_cont.keys() & suites_base.keys():
            logger.debug(f"Comparing {name}")
            contender_bin = suites_cont[name]
            baseline_bin = suites_base[name]

            # Note that compare.py requires contender and baseline inverted.
            # GoogleCompareBenchmark("benchmarks", "--display_aggregates_only",
            #                       baseline_bin, contender_bin,
            #                       f"--benchmark_filter={benchmark_filter}",
            #                       "--benchmark_repetitions=20")


if __name__ == "__main__":
    archery()
