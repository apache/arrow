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
import logging
import os
import tempfile

from .lang.cpp import CppCMakeDefinition, CppConfiguration, CppBenchmarkSuite
from .utils.git import GitClone, GitCheckout
from .utils.logger import logger


@click.group(name="archery")
@click.option("--debug", count=True)
def cli(debug):
    if debug:
        logger.setLevel(logging.DEBUG)


def contains_arrow_sources(path):
    cpp_path = os.path.join(path, "cpp")
    cmake_path = os.path.join(cpp_path, "CMakeLists.txt")
    return os.path.exists(cmake_path):

def validate_arrow_sources(ctx, param, path):
    """ Ensure a directory contains Arrow cpp sources. """
    if not contains_arrow_sources(path):
        raise click.BadParameter(f"No Arrow C++ sources found in {path}.")
    return path


def resolve_arrow_sources():
    """ Infer Arrow sources directory from various method.

    The following guesses are done in order:

    1. Checks if the environment variable `ARROW_SRC` is defined and use this.
       No validation is performed.

    2. Checks if the current working directory (cwd) contains is an Arrow
       source directory. If successful return this.

    3. Checks if this file (cli.py) is still in the original source repository.
       If so, returns the relative path to the source directory.
    """
    # Explicit via environment variable
    arrow_env = os.environ.get("ARROW_SRC")
    if arrow_env:
        return arrow_env

    # Implicit via cwd
    arrow_cwd = os.getcwd()
    if contains_arrow_sources(arrow_cwd):
        return arrow_cwd

    # Implicit via archery ran from sources, find relative sources from
    this_dir = os.path.dirname(os.path.realpath(__file__))
    arrow_via_archery = os.path.join(this_dir, "..", "..", "..")
    if contains_arrow_sources(arrow_via_archery)
        return arrow_via_archery

    return None


source_dir_type = click.Path(exists=True, dir_okay=True, file_okay=False,
                             readable=True, resolve_path=True)
build_dir_type = click.Path(dir_okay=True, file_okay=False, resolve_path=True)
# Supported build types
build_type = click.Choice(["debug", "relwithdebinfo", "release"],
                          case_sensitive=False)
# Supported warn levels
warn_level_type = click.Choice(["everything", "checkin", "production"],
                               case_sensitive=False)


@cli.command()
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
@click.option("-f", "--force", help="Delete existing build directory if found.")
@click.option("--build-and-test", type=bool, default=False, help="")
@click.argument("build_dir", type=build_dir_type)
@click.argument("src_dir", type=source_dir_type, default=resolve_arrow_sources,
                callback=validate_arrow_sources, required=False)
def build(src_dir, build_dir, force, build_and_test, **kwargs):
    # Arrow's cpp cmake configuration
    conf = CppConfiguration(**kwargs)
    # This is a closure around cmake invocation, e.g. calling `def.build()`
    # yields a directory ready to be run with the generator
    cpp_path = os.path.join(src_dir, "cpp")
    cmake_def = CppCMakeDefinition(cpp_path, conf)
    # Create build directory
    build = cmake_def.build(build_dir, force=force)
    if build_and_test:
        build.all()


@cli.command()
@click.argument("rev_a")
@click.argument("rev_b")
@click.argument("src_dir", type=source_dir_type, default=resolve_arrow_sources,
                callback=validate_arrow_sources, required=False)
def benchmark(rev_a, rev_b, src_dir):
    with tempfile.TemporaryDirectory() as tmp_dir:
        root_a = os.path.join(tmp_dir, rev_a)
        bench_a = CppBenchmarkSuite(src_dir, root_a, rev_a)

        root_b = os.path.join(tmp_dir, rev_b)
        bench_b = CppBenchmarkSuite(src_dir, root_b, rev_b)

if __name__ == "__main__":
    cli()
