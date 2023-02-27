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

import fnmatch
import gzip
import os
from pathlib import Path

import click

from .command import Bash, Command, default_bin
from ..compat import _get_module
from .cmake import CMake
from .git import git
from .logger import logger
from ..lang.cpp import CppCMakeDefinition, CppConfiguration
from ..lang.python import Autopep8, Flake8, CythonLint, NumpyDoc
from .rat import Rat, exclusion_from_globs
from .tmpdir import tmpdir


_archery_install_msg = (
    "Please install archery using: `pip install -e dev/archery[lint]`. "
)


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
               clang_tidy=False, iwyu=False, iwyu_all=False,
               fix=False):
    """ Run clang-format, cpplint and clang-tidy on cpp/ codebase. """
    logger.info("Running C++ linters")

    cmake = CMake()
    if not cmake.available:
        logger.error("cpp linter requested but cmake binary not found.")
        return

    # A cmake build directory is required to populate `compile_commands.json`
    # which in turn is required by clang-tidy. It also provides a convenient
    # way to hide clang-format/clang-tidy invocation via the Generate
    # (ninja/make) targets.

    # ARROW_LINT_ONLY exits early but ignore building compile_command.json
    lint_only = not (iwyu or clang_tidy)
    cmake_args = {"with_python": False, "with_lint_only": lint_only}
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
        if iwyu_all:
            iwyu_cmd = "iwyu-all"
        else:
            iwyu_cmd = "iwyu"
        yield LintResult.from_cmd(build.run(iwyu_cmd, check=False))


class CMakeFormat(Command):

    def __init__(self, paths, cmake_format_bin=None):
        self.check_version()
        self.bin = default_bin(cmake_format_bin, "cmake-format")
        self.paths = paths

    @classmethod
    def from_patterns(cls, base_path, include_patterns, exclude_patterns):
        paths = {
            str(path.as_posix())
            for pattern in include_patterns
            for path in base_path.glob(pattern)
        }
        for pattern in exclude_patterns:
            pattern = (base_path / pattern).as_posix()
            paths -= set(fnmatch.filter(paths, str(pattern)))
        return cls(paths)

    @staticmethod
    def check_version():
        try:
            # cmake_format is part of the cmakelang package
            import cmakelang
        except ImportError:
            raise ImportError(

            )
        # pin a specific version of cmake_format, must be updated in setup.py
        if cmakelang.__version__ != "0.6.13":
            raise LintValidationException(
                f"Wrong version of cmake_format is detected. "
                f"{_archery_install_msg}"
            )

    def check(self):
        return self.run("-l", "error", "--check", *self.paths, check=False)

    def fix(self):
        return self.run("--in-place", *self.paths, check=False)


def cmake_linter(src, fix=False):
    """
    Run cmake-format on all CMakeFiles.txt
    """
    logger.info("Running cmake-format linters")

    cmake_format = CMakeFormat.from_patterns(
        src.path,
        include_patterns=[
            'ci/**/*.cmake',
            'cpp/CMakeLists.txt',
            'cpp/src/**/*.cmake.in',
            'cpp/src/**/CMakeLists.txt',
            'cpp/examples/**/CMakeLists.txt',
            'cpp/cmake_modules/*.cmake',
            'go/**/CMakeLists.txt',
            'java/**/CMakeLists.txt',
            'matlab/**/CMakeLists.txt',
            'python/CMakeLists.txt',
        ],
        exclude_patterns=[
            'cpp/cmake_modules/FindNumPy.cmake',
            'cpp/cmake_modules/FindPythonLibsNew.cmake',
            'cpp/cmake_modules/UseCython.cmake',
            'cpp/src/arrow/util/config.h.cmake',
        ]
    )
    method = cmake_format.fix if fix else cmake_format.check

    yield LintResult.from_cmd(method())


def python_linter(src, fix=False):
    """Run Python linters on python/pyarrow, python/examples, setup.py
    and dev/. """
    setup_py = os.path.join(src.python, "setup.py")
    setup_cfg = os.path.join(src.python, "setup.cfg")

    logger.info("Running Python formatter (autopep8)")

    autopep8 = Autopep8()
    if not autopep8.available:
        logger.error(
            "Python formatter requested but autopep8 binary not found. "
            f"{_archery_install_msg}")
        return

    # Gather files for autopep8
    patterns = ["python/pyarrow/**/*.py",
                "python/pyarrow/**/*.pyx",
                "python/pyarrow/**/*.pxd",
                "python/pyarrow/**/*.pxi",
                "python/examples/**/*.py",
                "dev/*.py",
                "dev/archery/**/*.py",
                "dev/release/**/*.py"]
    files = [setup_py]
    for pattern in patterns:
        files += list(map(str, Path(src.path).glob(pattern)))

    args = ['--global-config', setup_cfg, '--ignore-local-config']
    if fix:
        args += ['-j0', '--in-place']
        args += sorted(files)
        yield LintResult.from_cmd(autopep8(*args))
    else:
        # XXX `-j0` doesn't work well with `--exit-code`, so instead
        # we capture the diff and check whether it's empty
        # (https://github.com/hhatto/autopep8/issues/543)
        args += ['-j0', '--diff']
        args += sorted(files)
        diff = autopep8.run_captured(*args)
        if diff:
            print(diff.decode('utf8'))
            yield LintResult(success=False)
        else:
            yield LintResult(success=True)

    # Run flake8 after autopep8 (the latter may have modified some files)
    logger.info("Running Python linter (flake8)")

    flake8 = Flake8()
    if not flake8.available:
        logger.error(
            "Python linter requested but flake8 binary not found. "
            f"{_archery_install_msg}")
        return

    flake8_exclude = ['.venv*', 'vendored']

    yield LintResult.from_cmd(
        flake8("--extend-exclude=" + ','.join(flake8_exclude),
               "--config=" + os.path.join(src.python, "setup.cfg"),
               setup_py, src.pyarrow, os.path.join(src.python, "examples"),
               src.dev, check=False))

    logger.info("Running Cython linter (cython-lint)")

    cython_lint = CythonLint()
    if not cython_lint.available:
        logger.error(
            "Cython linter requested but cython-lint binary not found. "
            f"{_archery_install_msg}")
        return

    # Gather files for cython-lint
    patterns = ["python/pyarrow/**/*.pyx",
                "python/pyarrow/**/*.pxd",
                "python/pyarrow/**/*.pxi",
                "python/examples/**/*.pyx",
                "python/examples/**/*.pxd",
                "python/examples/**/*.pxi",
                ]
    files = []
    for pattern in patterns:
        files += list(map(str, Path(src.path).glob(pattern)))
    args = ['--no-pycodestyle']
    args += sorted(files)
    yield LintResult.from_cmd(cython_lint(*args))


def python_numpydoc(symbols=None, allow_rules=None, disallow_rules=None):
    """Run numpydoc linter on python.

    Pyarrow must be available for import.
    """
    logger.info("Running Python docstring linters")
    # by default try to run on all pyarrow package
    symbols = symbols or {
        'pyarrow',
        'pyarrow.compute',
        'pyarrow.csv',
        'pyarrow.dataset',
        'pyarrow.feather',
        # 'pyarrow.flight',
        'pyarrow.fs',
        'pyarrow.gandiva',
        'pyarrow.ipc',
        'pyarrow.json',
        'pyarrow.orc',
        'pyarrow.parquet',
        'pyarrow.plasma',
        'pyarrow.types',
    }
    try:
        numpydoc = NumpyDoc(symbols)
    except RuntimeError as e:
        logger.error(str(e))
        yield LintResult(success=False)
        return

    results = numpydoc.validate(
        # limit the validation scope to the pyarrow package
        from_package='pyarrow',
        allow_rules=allow_rules,
        disallow_rules=disallow_rules
    )

    if len(results) == 0:
        yield LintResult(success=True)
        return

    number_of_violations = 0
    for obj, result in results:
        errors = result['errors']

        # inspect doesn't play nice with cython generated source code,
        # to use a hacky way to represent a proper __qualname__
        doc = getattr(obj, '__doc__', '')
        name = getattr(obj, '__name__', '')
        qualname = getattr(obj, '__qualname__', '')
        module = _get_module(obj, default='')
        instance = getattr(obj, '__self__', '')
        if instance:
            klass = instance.__class__.__name__
        else:
            klass = ''

        try:
            cython_signature = doc.splitlines()[0]
        except Exception:
            cython_signature = ''

        desc = '.'.join(filter(None, [module, klass, qualname or name]))

        click.echo()
        click.echo(click.style(desc, bold=True, fg='yellow'))
        if cython_signature:
            qualname_with_signature = '.'.join([module, cython_signature])
            click.echo(
                click.style(
                    '-> {}'.format(qualname_with_signature),
                    fg='yellow'
                )
            )

        for error in errors:
            number_of_violations += 1
            click.echo('{}: {}'.format(*error))

    msg = 'Total number of docstring violations: {}'.format(
        number_of_violations
    )
    click.echo()
    click.echo(click.style(msg, fg='red'))

    yield LintResult(success=False)


def rat_linter(src, root):
    """Run apache-rat license linter."""
    logger.info("Running apache-rat linter")

    if src.git_dirty:
        logger.warn("Due to the usage of git-archive, uncommitted files will"
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
        print("apache-rat license violation: {}".format(violation))

    yield LintResult(len(violations) == 0)


def r_linter(src):
    """Run R linter."""
    logger.info("Running R linter")
    r_lint_sh = os.path.join(src.r, "lint.sh")
    yield LintResult.from_cmd(Bash().run(r_lint_sh, check=False))


class Hadolint(Command):
    def __init__(self, hadolint_bin=None):
        self.bin = default_bin(hadolint_bin, "hadolint")


def is_docker_image(path):
    dirname = os.path.dirname(path)
    filename = os.path.basename(path)

    excluded = dirname.startswith(
        "dev") or dirname.startswith("python/manylinux")

    return filename.startswith("Dockerfile") and not excluded


def docker_linter(src):
    """Run Hadolint docker linter."""
    logger.info("Running Docker linter")

    hadolint = Hadolint()

    if not hadolint.available:
        logger.error(
            "hadolint linter requested but hadolint binary not found.")
        return

    for path in git.ls_files(git_dir=src.path):
        if is_docker_image(path):
            yield LintResult.from_cmd(hadolint.run(path, check=False,
                                                   cwd=src.path))


def linter(src, fix=False, *, clang_format=False, cpplint=False,
           clang_tidy=False, iwyu=False, iwyu_all=False,
           python=False, numpydoc=False, cmake_format=False, rat=False,
           r=False, docker=False):
    """Run all linters."""
    with tmpdir(prefix="arrow-lint-") as root:
        build_dir = os.path.join(root, "cpp-build")

        # Linters yield LintResult without raising exceptions on failure.
        # This allows running all linters in one pass and exposing all
        # errors to the user.
        results = []

        if clang_format or cpplint or clang_tidy or iwyu:
            results.extend(cpp_linter(src, build_dir,
                                      clang_format=clang_format,
                                      cpplint=cpplint,
                                      clang_tidy=clang_tidy,
                                      iwyu=iwyu,
                                      iwyu_all=iwyu_all,
                                      fix=fix))

        if python:
            results.extend(python_linter(src, fix=fix))

        if numpydoc:
            results.extend(python_numpydoc())

        if cmake_format:
            results.extend(cmake_linter(src, fix=fix))

        if rat:
            results.extend(rat_linter(src, root))

        if r:
            results.extend(r_linter(src))

        if docker:
            results.extend(docker_linter(src))

        # Raise error if one linter failed, ensuring calling code can exit with
        # non-zero.
        for result in results:
            result.ok()
