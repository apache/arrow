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

import gzip
import os

from .command import Bash, Command, default_bin
from .git import git
from .logger import logger
from ..lang.cpp import CppCMakeDefinition, CppConfiguration
from ..lang.rust import Cargo
from .rat import Rat, exclusion_from_globs
from .tmpdir import tmpdir


class LintValidationException(Exception):
    pass


class LintResult:
    def __init__(self, success, reason=None):
        self.success = success

    def ok(self):
        if not self.success:
            raise LintValidationException

    @staticmethod
    def from_cmd(command_result):
        return LintResult(command_result.returncode == 0)


def cpp_linter(src, build_dir, clang_format=True, cpplint=True,
               clang_tidy=False, iwyu=False, fix=False):
    """ Run clang-format, cpplint and clang-tidy on cpp/ codebase. """
    logger.info("Running C++ linters")

    # A cmake build directory is required to populate `compile_commands.json`
    # which in turn is required by clang-tidy. It also provides a convenient
    # way to hide clang-format/clang-tidy invocation via the Generate
    # (ninja/make) targets.
    cmake_args = {"with_python": False}
    cmake_def = CppCMakeDefinition(src.cpp, CppConfiguration(**cmake_args))

    build = cmake_def.build(build_dir)
    if clang_format:
        target = "format" if fix else "check-format"
        yield LintResult.from_cmd(build.run(target, check=False))

    if cpplint:
        yield LintResult.from_cmd(build.run("lint", check=False))
        yield LintResult.from_cmd(build.run("lint_cpp_cli", check=False))

    if clang_tidy:
        yield LintResult.from_cmd(build.run("check-clang-tidy", check=False))

    if iwyu:
        yield LintResult.from_cmd(build.run("iwyu", check=False))


class CMakeFormat(Command):
    def __init__(self, cmake_format_bin):
        self.bin = cmake_format_bin


def cmake_linter(src, fix=False):
    """ Run cmake-format.py on all CMakeFiles.txt """
    logger.info("Running cmake-format linters")
    if not fix:
        logger.warn("run-cmake-format modifies files, irregardless of --fix")
    arrow_cmake_format = os.path.join(src.path, "run-cmake-format.py")
    cmake_format = CMakeFormat(cmake_format_bin=arrow_cmake_format)
    yield LintResult.from_cmd(cmake_format("--check"))


class Flake8(Command):
    def __init__(self, flake8_bin=None):
        self.bin = default_bin(flake8_bin, "FLAKE8", "flake8")


def python_linter(src):
    """ Run flake8 linter on python/pyarrow, and dev/. """
    logger.info("Running python linters")
    flake8 = Flake8()

    yield LintResult.from_cmd(flake8(src.pyarrow, src.dev, check=False))
    config = os.path.join(src.pyarrow, ".flake8.cython")
    yield LintResult.from_cmd(flake8("--config=" + config, src.pyarrow,
                                     check=False))


def rat_linter(src, root):
    """ Run apache-rat license linter. """
    logger.info("Running apache-rat linter")

    if src.git_dirty:
        logger.warn("Due to the usage of git-archive, uncommited files will"
                    " not be checked for rat violations. ")

    exclusion = exclusion_from_globs(
        os.path.join(src.dev, "release", "rat_exclude_files.txt"))

    # Creates a git-archive of ArrowSources, apache-rat expects a gzip
    # compressed tar archive.
    archive_path = os.path.join(root, "apache-arrow.tar.gz")
    src.archive(archive_path, compressor=gzip.compress)
    report = Rat().report(archive_path)

    violations = list(report.validate(exclusion=exclusion))
    for violation in violations:
        print(f"apache-rat license violation: {violation}")

    yield LintResult(len(violations) == 0)


def r_linter(src):
    """ Run R linter. """
    logger.info("Running r linter")
    r_lint_sh = os.path.join(src.r, "lint.sh")
    yield LintResult.from_cmd(Bash().run(r_lint_sh, check=False))


def rust_linter(src):
    """ Run Rust linter. """
    logger.info("Running rust linter")
    yield LintResult.from_cmd(Cargo().run("+stable", "fmt", "--all", "--",
                                          "--check", cwd=src.rust,
                                          check=False))


class Hadolint(Command):
    def __init__(self, hadolint_bin=None):
        self.bin = default_bin(hadolint_bin, "HADOLINT", "hadolint")


def is_docker_image(path):
    dirname = os.path.dirname(path)
    filename = os.path.basename(path)

    excluded = dirname.startswith(
        "dev") or dirname.startswith("python/manylinux")

    return filename.startswith("Dockerfile") and not excluded


def docker_linter(src):
    """ Run Hadolint docker linter. """
    logger.info("Running docker linter")

    for path in git.ls_files(git_dir=src.path):
        if is_docker_image(path):
            yield LintResult.from_cmd(Hadolint().run(path, check=False,
                                                     cwd=src.path))


def linter(src, with_clang_format=True, with_cpplint=True,
           with_clang_tidy=False, with_iwyu=False,
           with_flake8=True, with_cmake_format=True,
           with_rat=True, with_r=True, with_rust=True,
           with_docker=True,
           fix=False):
    """ Run all linters. """
    with tmpdir(prefix="arrow-lint-") as root:
        build_dir = os.path.join(root, "cpp-build")

        # Linters yield LintResult without raising exceptions on failure.
        # This allows running all linters in one pass and exposing all
        # errors to the user.
        results = []

        if with_clang_format or with_cpplint or with_clang_tidy or with_iwyu:
            results.extend(cpp_linter(src, build_dir,
                                      clang_format=with_clang_format,
                                      cpplint=with_cpplint,
                                      clang_tidy=with_clang_tidy,
                                      iwyu=with_iwyu,
                                      fix=fix))

        if with_flake8:
            results.extend(python_linter(src))

        if with_cmake_format:
            results.extend(cmake_linter(src, fix=fix))

        if with_rat:
            results.extend(rat_linter(src, root))

        if with_r:
            results.extend(r_linter(src))

        if with_rust:
            results.extend(rust_linter(src))

        if with_docker:
            results.extend(docker_linter(src))

        # Raise error if one linter failed, ensuring calling code can exit with
        # non-zero.
        for result in results:
            result.ok()
