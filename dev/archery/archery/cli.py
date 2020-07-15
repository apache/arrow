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

from collections import namedtuple
from io import StringIO
import click
import errno
import json
import logging
import os
import pathlib
import sys

from .benchmark.compare import RunnerComparator, DEFAULT_THRESHOLD
from .benchmark.runner import BenchmarkRunner, CppBenchmarkRunner
from .lang.cpp import CppCMakeDefinition, CppConfiguration
from .utils.codec import JsonEncoder
from .utils.lint import linter, python_numpydoc, LintValidationException
from .utils.logger import logger, ctx as log_ctx
from .utils.source import ArrowSources, InvalidArrowSource
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
    try:
        return ArrowSources.find(src)
    except InvalidArrowSource as e:
        raise click.BadParameter(str(e))


build_dir_type = click.Path(dir_okay=True, file_okay=False, resolve_path=True)
# Supported build types
build_type = click.Choice(["debug", "relwithdebinfo", "release"],
                          case_sensitive=False)
# Supported warn levels
warn_level_type = click.Choice(["everything", "checkin", "production"],
                               case_sensitive=False)

simd_level = click.Choice(["NONE", "SSE4_2", "AVX2", "AVX512"],
                          case_sensitive=True)


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
@click.option("--src", metavar="<arrow_src>", default=None,
              callback=validate_arrow_sources,
              help="Specify Arrow source directory")
# toolchain
@cpp_toolchain_options
@click.option("--build-type", default=None, type=build_type,
              help="CMake's CMAKE_BUILD_TYPE")
@click.option("--warn-level", default="production", type=warn_level_type,
              help="Controls compiler warnings -W(no-)error.")
@click.option("--use-gold-linker", default=True, type=BOOL,
              help="Toggles ARROW_USE_LD_GOLD option.")
@click.option("--simd-level", default="SSE4_2", type=simd_level,
              help="Toggles ARROW_SIMD_LEVEL option.")
# Tests and benchmarks
@click.option("--with-tests", default=True, type=BOOL,
              help="Build with tests.")
@click.option("--with-benchmarks", default=None, type=BOOL,
              help="Build with benchmarks.")
@click.option("--with-examples", default=None, type=BOOL,
              help="Build with examples.")
@click.option("--with-integration", default=None, type=BOOL,
              help="Build with integration test executables.")
# Static checks
@click.option("--use-asan", default=None, type=BOOL,
              help="Toggle ARROW_USE_ASAN sanitizer.")
@click.option("--use-tsan", default=None, type=BOOL,
              help="Toggle ARROW_USE_TSAN sanitizer.")
@click.option("--use-ubsan", default=None, type=BOOL,
              help="Toggle ARROW_USE_UBSAN sanitizer.")
@click.option("--with-fuzzing", default=None, type=BOOL,
              help="Toggle ARROW_FUZZING.")
# Components
@click.option("--with-compute", default=None, type=BOOL,
              help="Build the Arrow compute module.")
@click.option("--with-csv", default=None, type=BOOL,
              help="Build the Arrow CSV parser module.")
@click.option("--with-cuda", default=None, type=BOOL,
              help="Build the Arrow CUDA extensions.")
@click.option("--with-dataset", default=None, type=BOOL,
              help="Build the Arrow dataset module.")
@click.option("--with-filesystem", default=None, type=BOOL,
              help="Build the Arrow filesystem layer.")
@click.option("--with-flight", default=None, type=BOOL,
              help="Build with Flight rpc support.")
@click.option("--with-gandiva", default=None, type=BOOL,
              help="Build with Gandiva expression compiler support.")
@click.option("--with-hdfs", default=None, type=BOOL,
              help="Build the Arrow HDFS bridge.")
@click.option("--with-hiveserver2", default=None, type=BOOL,
              help="Build the HiveServer2 client and arrow adapater.")
@click.option("--with-ipc", default=None, type=BOOL,
              help="Build the Arrow IPC extensions.")
@click.option("--with-json", default=None, type=BOOL,
              help="Build the Arrow JSON parser module.")
@click.option("--with-jni", default=None, type=BOOL,
              help="Build the Arrow JNI lib.")
@click.option("--with-mimalloc", default=None, type=BOOL,
              help="Build the Arrow mimalloc based allocator.")
@click.option("--with-parquet", default=None, type=BOOL,
              help="Build with Parquet file support.")
@click.option("--with-plasma", default=None, type=BOOL,
              help="Build with Plasma object store support.")
@click.option("--with-python", default=None, type=BOOL,
              help="Build the Arrow CPython extesions.")
@click.option("--with-r", default=None, type=BOOL,
              help="Build the Arrow R extensions. This is not a CMake option, "
              "it will toggle required options")
@click.option("--with-s3", default=None, type=BOOL,
              help="Build Arrow with S3 support.")
# Compressions
@click.option("--with-brotli", default=None, type=BOOL,
              help="Build Arrow with brotli compression.")
@click.option("--with-bz2", default=None, type=BOOL,
              help="Build Arrow with bz2 compression.")
@click.option("--with-lz4", default=None, type=BOOL,
              help="Build Arrow with lz4 compression.")
@click.option("--with-snappy", default=None, type=BOOL,
              help="Build Arrow with snappy compression.")
@click.option("--with-zlib", default=None, type=BOOL,
              help="Build Arrow with zlib compression.")
@click.option("--with-zstd", default=None, type=BOOL,
              help="Build Arrow with zstd compression.")
# CMake extra feature
@click.option("--cmake-extras", type=str, multiple=True,
              help="Extra flags/options to pass to cmake invocation. "
              "Can be stacked")
@click.option("--install-prefix", type=str,
              help="Destination directory where files are installed. Expand to"
              "CMAKE_INSTALL_PREFIX. Defaults to to $CONDA_PREFIX if the"
              "variable exists.")
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
    # Initialize build with clang8 and avx2 support in directory `clang8-build`
    \b
    archery build --cc=clang-8 --cxx=clang++-8 --cxx-flags=-mavx2 clang8-build

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


LintCheck = namedtuple('LintCheck', ('option_name', 'help'))

lint_checks = [
    LintCheck('clang-format', "Format C++ files with clang-format."),
    LintCheck('clang-tidy', "Lint C++ files with clang-tidy."),
    LintCheck('cpplint', "Lint C++ files with cpplint."),
    LintCheck('iwyu', "Lint changed C++ files with Include-What-You-Use."),
    LintCheck('python',
              "Format and lint Python files with autopep8 and flake8."),
    LintCheck('numpydoc', "Lint Python files with numpydoc."),
    LintCheck('cmake-format', "Format CMake files with cmake-format.py."),
    LintCheck('rat',
              "Check all sources files for license texts via Apache RAT."),
    LintCheck('r', "Lint R files."),
    LintCheck('rust', "Lint Rust files."),
    LintCheck('docker', "Lint Dockerfiles with hadolint."),
]


def decorate_lint_command(cmd):
    """
    Decorate the lint() command function to add individual per-check options.
    """
    for check in lint_checks:
        option = click.option("--{0}/--no-{0}".format(check.option_name),
                              default=None, help=check.help)
        cmd = option(cmd)
    return cmd


@archery.command(short_help="Check Arrow source tree for errors")
@click.option("--src", metavar="<arrow_src>", default=None,
              callback=validate_arrow_sources,
              help="Specify Arrow source directory")
@click.option("--fix", is_flag=True, type=BOOL, default=False,
              help="Toggle fixing the lint errors if the linter supports it.")
@click.option("--iwyu_all", is_flag=True, type=BOOL, default=False,
              help="Run IWYU on all C++ files if enabled")
@click.option("-a", "--all", is_flag=True, default=False,
              help="Enable all checks.")
@decorate_lint_command
@click.pass_context
def lint(ctx, src, fix, iwyu_all, **checks):
    if checks.pop('all'):
        # "--all" is given => enable all non-selected checks
        for k, v in checks.items():
            if v is None:
                checks[k] = True
    if not any(checks.values()):
        raise click.UsageError(
            "Need to enable at least one lint check (try --help)")
    try:
        linter(src, fix, iwyu_all=iwyu_all, **checks)
    except LintValidationException:
        sys.exit(1)


@archery.command(short_help="Lint python docstring with NumpyDoc")
@click.argument('symbols', nargs=-1)
@click.option("--src", metavar="<arrow_src>", default=None,
              callback=validate_arrow_sources,
              help="Specify Arrow source directory")
@click.option("--allow-rule", "-a", multiple=True,
              help="Allow only these rules")
@click.option("--disallow-rule", "-d", multiple=True,
              help="Disallow these rules")
def numpydoc(src, symbols, allow_rule, disallow_rule):
    """
    Pass list of modules or symbols as arguments to restrict the validation.

    By default all modules of pyarrow are tried to be validated.

    Examples
    --------
    archery numpydoc pyarrow.dataset
    archery numpydoc pyarrow.csv pyarrow.json pyarrow.parquet
    archery numpydoc pyarrow.array
    """
    disallow_rule = disallow_rule or {'GL01', 'SA01', 'EX01', 'ES01'}
    try:
        results = python_numpydoc(symbols, allow_rules=allow_rule,
                                  disallow_rule=disallow_rule)
        for result in results:
            result.ok()
    except LintValidationException:
        sys.exit(1)


@archery.group()
@click.pass_context
def benchmark(ctx):
    """ Arrow benchmarking.

    Use the diff sub-command to benchmark revisions, and/or build directories.
    """
    pass


def benchmark_common_options(cmd):
    options = [
        click.option("--src", metavar="<arrow_src>", show_default=True,
                     default=None, callback=validate_arrow_sources,
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
                     help="Regex filtering benchmarks.")
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
        logger.debug("Running benchmark {}".format(rev_or_path))

        conf = CppBenchmarkRunner.default_configuration(
            cmake_extras=cmake_extras, **kwargs)

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
        logger.debug("Running benchmark {}".format(rev_or_path))

        conf = CppBenchmarkRunner.default_configuration(
            cmake_extras=cmake_extras, **kwargs)

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
@click.option("--repetitions", type=int, default=1, show_default=True,
              help=("Number of repetitions of each benchmark. Increasing "
                    "may improve result precision."))
@click.argument("contender", metavar="[<contender>",
                default=ArrowSources.WORKSPACE, required=False)
@click.argument("baseline", metavar="[<baseline>]]", default="origin/master",
                required=False)
@click.pass_context
def benchmark_diff(ctx, src, preserve, output, cmake_extras,
                   suite_filter, benchmark_filter,
                   repetitions, threshold, contender, baseline, **kwargs):
    """Compare (diff) benchmark runs.

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
    # Compare g++7 (contender) with clang++-8 (baseline) builds
    \b
    archery build --with-benchmarks=true \\
            --cxx-flags=-ftree-vectorize \\
            --cc=gcc-7 --cxx=g++-7 gcc7-build
    \b
    archery build --with-benchmarks=true \\
            --cxx-flags=-flax-vector-conversions \\
            --cc=clang-8 --cxx=clang++-8 clang8-build
    \b
    archery benchmark diff gcc7-build clang8-build

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
        logger.debug("Comparing {} (contender) with {} (baseline)"
                     .format(contender, baseline))

        conf = CppBenchmarkRunner.default_configuration(
            cmake_extras=cmake_extras, **kwargs)

        runner_cont = BenchmarkRunner.from_rev_or_path(
            src, root, contender, conf,
            repetitions=repetitions,
            suite_filter=suite_filter,
            benchmark_filter=benchmark_filter)
        runner_base = BenchmarkRunner.from_rev_or_path(
            src, root, baseline, conf,
            repetitions=repetitions,
            suite_filter=suite_filter,
            benchmark_filter=benchmark_filter)

        runner_comp = RunnerComparator(runner_cont, runner_base, threshold)

        # TODO(kszucs): test that the output is properly formatted jsonlines
        comparisons_json = _get_comparisons_as_json(runner_comp.comparisons)
        formatted = _format_comparisons_with_pandas(comparisons_json)
        output.write(formatted)
        output.write('\n')


def _get_comparisons_as_json(comparisons):
    buf = StringIO()
    for comparator in comparisons:
        json.dump(comparator, buf, cls=JsonEncoder)
        buf.write("\n")

    return buf.getvalue()


def _format_comparisons_with_pandas(comparisons_json):
    import pandas as pd
    df = pd.read_json(StringIO(comparisons_json), lines=True)
    # parse change % so we can sort by it
    df['change %'] = df.pop('change').str[:-1].map(float)
    df = df[['benchmark', 'baseline', 'contender', 'change %', 'counters']]
    df = df.sort_values(by='change %', ascending=False)
    return df.to_string()


# ----------------------------------------------------------------------
# Integration testing

def _set_default(opt, default):
    if opt is None:
        return default
    return opt


@archery.command(short_help="Execute protocol and Flight integration tests")
@click.option('--with-all', is_flag=True, default=False,
              help=('Include all known languages by default '
                    'in integration tests'))
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
@click.option('--with-rust', type=bool, default=False,
              help='Include Rust in integration tests')
@click.option('--write_generated_json', default=False,
              help='Generate test JSON to indicated path')
@click.option('--run-flight', is_flag=True, default=False,
              help='Run Flight integration tests')
@click.option('--debug', is_flag=True, default=False,
              help='Run executables in debug mode as relevant')
@click.option('--serial', is_flag=True, default=False,
              help='Run tests serially, rather than in parallel')
@click.option('--tempdir', default=None,
              help=('Directory to use for writing '
                    'integration test temporary files'))
@click.option('stop_on_error', '-x', '--stop-on-error',
              is_flag=True, default=False,
              help='Stop on first error')
@click.option('--gold-dirs', multiple=True,
              help="gold integration test file paths")
@click.option('-k', '--match',
              help=("Substring for test names to include in run, "
                    "e.g. -k primitive"))
def integration(with_all=False, random_seed=12345, **args):
    from .integration.runner import write_js_test_json, run_all_tests
    import numpy as np

    # FIXME(bkietz) Include help strings for individual testers.
    # For example, CPPTester's ARROW_CPP_EXE_PATH environment variable.

    # Make runs involving data generation deterministic
    np.random.seed(random_seed)

    gen_path = args['write_generated_json']

    languages = ['cpp', 'java', 'js', 'go', 'rust']

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


@archery.command()
@click.option('--event-name', '-n', required=True)
@click.option('--event-payload', '-p', type=click.File('r', encoding='utf8'),
              default='-', required=True)
@click.option('--arrow-token', envvar='ARROW_GITHUB_TOKEN',
              help='OAuth token for responding comment in the arrow repo')
@click.option('--crossbow-token', '-ct', envvar='CROSSBOW_GITHUB_TOKEN',
              help='OAuth token for pushing to the crossow repository')
def trigger_bot(event_name, event_payload, arrow_token, crossbow_token):
    from .bot import CommentBot, actions

    event_payload = json.loads(event_payload.read())

    bot = CommentBot(name='github-actions', handler=actions, token=arrow_token)
    bot.handle(event_name, event_payload)


@archery.group('docker')
@click.option("--src", metavar="<arrow_src>", default=None,
              callback=validate_arrow_sources,
              help="Specify Arrow source directory.")
@click.pass_obj
def docker_compose(obj, src):
    """Interact with docker-compose based builds."""
    from .docker import DockerCompose

    config_path = src.path / 'docker-compose.yml'
    if not config_path.exists():
        raise click.ClickException(
            "Docker compose configuration cannot be found in directory {}, "
            "try to pass the arrow source directory explicitly.".format(src)
        )

    # take the docker-compose parameters like PYTHON, PANDAS, UBUNTU from the
    # environment variables to keep the usage similar to docker-compose
    obj['compose'] = DockerCompose(config_path, params=os.environ)


@docker_compose.command('run')
@click.argument('image')
@click.argument('command', required=False, default=None)
@click.option('--env', '-e', multiple=True,
              help="Set environment variable within the container")
@click.option('--user', '-u', default=None,
              help="Username or UID to run the container with")
@click.option('--force-pull/--no-pull', default=True,
              help="Whether to force pull the image and its ancestor images")
@click.option('--force-build/--no-build', default=True,
              help="Whether to force build the image and its ancestor images")
@click.option('--build-only', default=False, is_flag=True,
              help="Pull and/or build the image, but do not run it")
@click.option('--use-cache/--no-cache', default=True,
              help="Whether to use cache when building the image and its "
                   "ancestor images")
@click.option('--use-leaf-cache/--no-leaf-cache', default=True,
              help="Whether to use cache when building only the (leaf) image "
                   "passed as the argument. To disable caching for both the "
                   "image and its ancestors use --no-cache option.")
@click.option('--dry-run/--execute', default=False,
              help="Display the docker-compose commands instead of executing "
                   "them.")
@click.option('--volume', '-v', multiple=True,
              help="Set volume within the container")
@click.pass_obj
def docker_compose_run(obj, image, command, *, env, user, force_pull,
                       force_build, build_only, use_cache, use_leaf_cache,
                       dry_run, volume):
    """Execute docker-compose builds.

    To see the available builds run `archery docker images`.

    Examples:

    # execute a single build
    archery docker run conda-python

    # execute the builds but disable the image pulling
    archery docker run --no-cache conda-python

    # pass a docker-compose parameter, like the python version
    PYTHON=3.8 archery docker run conda-python

    # disable the cache only for the leaf image
    PANDAS=master archery docker run --no-leaf-cache conda-python-pandas

    # entirely skip building the image
    archery docker run --no-pull --no-build conda-python

    # pass runtime parameters via docker environment variables
    archery docker run -e CMAKE_BUILD_TYPE=release ubuntu-cpp

    # set a volume
    archery docker run -v $PWD/build:/build ubuntu-cpp

    # starting an interactive bash session for debugging
    archery docker run ubuntu-cpp bash
    """
    from .docker import UndefinedImage

    compose = obj['compose']

    if dry_run:
        from types import MethodType

        def _print_command(self, *args, **kwargs):
            params = ['{}={}'.format(k, v) for k, v in self.params.items()]
            command = ' '.join(params + ['docker-compose'] + list(args))
            click.echo(command)

        compose._execute_compose = MethodType(_print_command, compose)

    env = dict(kv.split('=', 1) for kv in env)
    try:
        compose.run(
            image,
            command=command,
            env=env,
            user=user,
            force_pull=force_pull,
            force_build=force_build,
            build_only=build_only,
            use_cache=use_cache,
            use_leaf_cache=use_leaf_cache,
            volumes=volume
        )
    except UndefinedImage as e:
        raise click.ClickException(
            "There is no service/image defined in docker-compose.yml with "
            "name: {}".format(str(e))
        )
    except RuntimeError as e:
        raise click.ClickException(str(e))


@docker_compose.command('push')
@click.argument('image')
@click.option('--user', '-u', required=True, envvar='ARCHERY_DOCKER_USER',
              help='Docker repository username')
@click.option('--password', '-p', required=True,
              envvar='ARCHERY_DOCKER_PASSWORD',
              help='Docker repository password')
@click.pass_obj
def docker_compose_push(obj, image, user, password):
    """Push the generated docker-compose image."""
    compose = obj['compose']
    compose.push(image, user=user, password=password)


@docker_compose.command('images')
@click.pass_obj
def docker_compose_images(obj):
    """List the available docker-compose images."""
    compose = obj['compose']
    click.echo('Available images:')
    for image in compose.images():
        click.echo(' - {}'.format(image))


@archery.group('release')
@click.option("--src", metavar="<arrow_src>", default=None,
              callback=validate_arrow_sources,
              help="Specify Arrow source directory.")
@click.option("--jira-cache", type=click.Path(), default=None,
              help="File path to cache queried JIRA issues per version.")
@click.pass_obj
def release(obj, src, jira_cache):
    """Release releated commands."""
    from .release import Jira, CachedJira

    jira = Jira()
    if jira_cache is not None:
        jira = CachedJira(jira_cache, jira=jira)

    obj['jira'] = jira
    obj['repo'] = src.path


@release.command('curate')
@click.argument('version')
@click.pass_obj
def release_curate(obj, version):
    """Release curation."""
    from .release import Release

    release = Release.from_jira(version, jira=obj['jira'], repo=obj['repo'])
    curation = release.curate()

    click.echo(curation.render('console'))


@release.group('changelog')
def release_changelog():
    """Release changelog."""
    pass


@release_changelog.command('add')
@click.argument('version')
@click.pass_obj
def release_changelog_add(obj, version):
    """Prepend the changelog with the current release"""
    from .release import Release

    jira, repo = obj['jira'], obj['repo']

    # just handle the current version
    release = Release.from_jira(version, jira=jira, repo=repo)
    if release.is_released:
        raise ValueError('This version has been already released!')

    changelog = release.changelog()
    changelog_path = pathlib.Path(repo) / 'CHANGELOG.md'

    current_content = changelog_path.read_text()
    new_content = changelog.render('markdown') + current_content

    changelog_path.write_text(new_content)
    click.echo("CHANGELOG.md is updated!")


@release_changelog.command('regenerate')
@click.pass_obj
def release_changelog_regenerate(obj):
    """Regeneretate the whole CHANGELOG.md file"""
    from .release import Release

    jira, repo = obj['jira'], obj['repo']
    changelogs = []

    for version in jira.arrow_versions():
        if not version.released:
            continue
        release = Release.from_jira(version, jira=jira, repo=repo)
        click.echo('Querying changelog for version: {}'.format(version))
        changelogs.append(release.changelog())

    click.echo('Rendering new CHANGELOG.md file...')
    changelog_path = pathlib.Path(repo) / 'CHANGELOG.md'
    with changelog_path.open('w') as fp:
        for cl in changelogs:
            fp.write(cl.render('markdown'))


@release.command('cherry-pick')
@click.pass_obj
def release_cherry_pick(obj):
    """Cherry pick commits."""
    from .release import PatchRelease

    release = obj['release']
    if not isinstance(release, PatchRelease):
        raise click.UsageError('Cherry-pick command only supported for patch '
                               'releases')

    commands = release.generate_update_branch_commands()
    for cmd in commands:
        click.echo(cmd)


if __name__ == "__main__":
    archery(obj={})
