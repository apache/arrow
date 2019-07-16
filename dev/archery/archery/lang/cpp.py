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


class CppConfiguration:
    def __init__(self,
                 # toolchain
                 cc=None, cxx=None, cxx_flags=None,
                 build_type=None, warn_level=None,
                 install_prefix=None, use_conda=None,
                 # components
                 with_tests=True, with_benchmarks=False, with_python=True,
                 with_parquet=False, with_gandiva=False, with_plasma=False,
                 with_flight=False, cmake_extras=None):
        self.cc = cc
        self.cxx = cxx
        self.cxx_flags = cxx_flags

        self.build_type = build_type
        self.warn_level = warn_level
        self._install_prefix = install_prefix
        self._use_conda = use_conda

        self.with_tests = with_tests
        self.with_benchmarks = with_benchmarks
        self.with_python = with_python
        self.with_parquet = with_parquet
        self.with_gandiva = with_gandiva
        self.with_plasma = with_plasma
        self.with_flight = with_flight
        self.cmake_extras = cmake_extras

    def _gen_defs(self):
        if self.cxx_flags:
            yield ("ARROW_CXXFLAGS", self.cxx_flags)

        yield ("CMAKE_EXPORT_COMPILE_COMMANDS", truthifier(True))
        yield ("CMAKE_BUILD_TYPE", or_else(self.build_type, "debug"))
        yield ("BUILD_WARNING_LEVEL", or_else(self.warn_level, "production"))

        # if not ctx.quiet:
        #   yield ("ARROW_VERBOSE_THIRDPARTY_BUILD", "ON")

        maybe_prefix = self.install_prefix
        if maybe_prefix:
            yield ("CMAKE_INSTALL_PREFIX", maybe_prefix)

        yield ("ARROW_BUILD_TESTS", truthifier(self.with_tests))
        yield ("ARROW_BUILD_BENCHMARKS", truthifier(self.with_benchmarks))

        yield ("ARROW_PYTHON", truthifier(self.with_python))
        yield ("ARROW_PARQUET", truthifier(self.with_parquet))
        yield ("ARROW_GANDIVA", truthifier(self.with_gandiva))
        yield ("ARROW_PLASMA", truthifier(self.with_plasma))
        yield ("ARROW_FLIGHT", truthifier(self.with_flight))

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
