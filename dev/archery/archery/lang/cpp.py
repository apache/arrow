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

import os

from ..utils.cmake import CMakeDefinition


def truthifier(value):
    return "ON" if value else "OFF"


def or_else(value, default):
    return value if value else default


LLVM_VERSION = 7


class CppConfiguration:

    def __init__(self,
                 # toolchain
                 cc=None, cxx=None, cxx_flags=None,
                 build_type=None, warn_level=None,
                 cpp_package_prefix=None, install_prefix=None, use_conda=None,
                 # tests & examples
                 with_tests=True, with_benchmarks=False, with_examples=False,
                 # Languages support
                 with_python=True,
                 # Format support
                 with_parquet=False,
                 # Components
                 with_gandiva=False, with_compute=False, with_dataset=False,
                 with_plasma=False, with_flight=False,
                 # extras
                 with_lint_only=False, with_fuzzing=False,
                 use_gold_linker=True, use_sanitizers=True,
                 cmake_extras=None):
        self._cc = cc
        self._cxx = cxx
        self.cxx_flags = cxx_flags

        self._build_type = build_type
        self.warn_level = warn_level
        self._install_prefix = install_prefix
        self._package_prefix = cpp_package_prefix
        self._use_conda = use_conda

        self.with_tests = with_tests
        self.with_benchmarks = with_benchmarks
        self.with_examples = with_examples
        self.with_python = with_python
        self.with_parquet = with_parquet or with_dataset
        self.with_gandiva = with_gandiva
        self.with_plasma = with_plasma
        self.with_flight = with_flight
        self.with_compute = with_compute
        self.with_dataset = with_dataset

        self.with_lint_only = with_lint_only
        self.with_fuzzing = with_fuzzing
        self.use_gold_linker = use_gold_linker
        self.use_sanitizers = use_sanitizers

        self.cmake_extras = cmake_extras

    @property
    def build_type(self):
        if self._build_type:
            return self._build_type

        if self.with_fuzzing:
            return "relwithdebinfo"

        return "release"

    @property
    def cc(self):
        if self._cc:
            return self._cc

        if self.with_fuzzing:
            return f"clang-{LLVM_VERSION}"

        return None

    @property
    def cxx(self):
        if self._cxx:
            return self._cxx

        if self.with_fuzzing:
            return f"clang++-{LLVM_VERSION}"

        return None

    def _gen_defs(self):
        if self.cxx_flags:
            yield ("ARROW_CXXFLAGS", self.cxx_flags)

        yield ("CMAKE_EXPORT_COMPILE_COMMANDS", truthifier(True))
        yield ("CMAKE_BUILD_TYPE", self.build_type)

        if not self.with_lint_only:
            yield ("BUILD_WARNING_LEVEL",
                   or_else(self.warn_level, "production"))

        # if not ctx.quiet:
        #   yield ("ARROW_VERBOSE_THIRDPARTY_BUILD", "ON")

        maybe_prefix = self.install_prefix
        if maybe_prefix:
            yield ("CMAKE_INSTALL_PREFIX", maybe_prefix)

        if self._package_prefix is not None:
            yield ("ARROW_DEPENDENCY_SOURCE", "SYSTEM")
            yield ("ARROW_PACKAGE_PREFIX", self._package_prefix)

        yield ("ARROW_BUILD_TESTS", truthifier(self.with_tests))
        yield ("ARROW_BUILD_BENCHMARKS", truthifier(self.with_benchmarks))
        yield ("ARROW_BUILD_EXAMPLES", truthifier(self.with_examples))

        yield ("ARROW_PYTHON", truthifier(self.with_python))

        if self.with_parquet:
            yield ("ARROW_PARQUET", truthifier(self.with_parquet))
            yield ("ARROW_WITH_BROTLI", "ON")
            yield ("ARROW_WITH_SNAPPY", "ON")

        yield ("ARROW_GANDIVA", truthifier(self.with_gandiva))
        yield ("ARROW_PLASMA", truthifier(self.with_plasma))
        yield ("ARROW_FLIGHT", truthifier(self.with_flight))
        yield ("ARROW_COMPUTE", truthifier(self.with_compute))
        yield ("ARROW_DATASET", truthifier(self.with_dataset))

        if self.use_sanitizers or self.with_fuzzing:
            yield ("ARROW_USE_ASAN", "ON")
            yield ("ARROW_USE_UBSAN", "ON")

        yield ("ARROW_LINT_ONLY", truthifier(self.with_lint_only))
        yield ("ARROW_FUZZING", truthifier(self.with_fuzzing))

        if self.use_gold_linker and not self.with_fuzzing:
            yield ("ARROW_USE_LD_GOLD", truthifier(self.use_gold_linker))

        # Detect custom conda toolchain
        if self.use_conda:
            for d, v in [('CMAKE_AR', 'AR'), ('CMAKE_RANLIB', 'RANLIB')]:
                v = os.environ.get(v)
                if v:
                    yield (d, v)

    @property
    def install_prefix(self):
        if self._install_prefix:
            return self._install_prefix

        if self.use_conda:
            return os.environ.get("CONDA_PREFIX")

        return None

    @property
    def use_conda(self):
        # If the user didn't specify a preference, guess via environment
        if self._use_conda is None:
            return os.environ.get("CONDA_PREFIX") is not None

        return self._use_conda

    @property
    def definitions(self):
        extras = list(self.cmake_extras) if self.cmake_extras else []
        return [f"-D{d[0]}={d[1]}" for d in self._gen_defs()] + extras

    @property
    def environment(self):
        env = os.environ.copy()

        if self.cc:
            env["CC"] = self.cc

        if self.cxx:
            env["CXX"] = self.cxx

        return env


class CppCMakeDefinition(CMakeDefinition):
    def __init__(self, source, conf, **kwargs):
        self.configuration = conf
        super().__init__(source, **kwargs,
                         definitions=conf.definitions, env=conf.environment,
                         build_type=conf.build_type)
