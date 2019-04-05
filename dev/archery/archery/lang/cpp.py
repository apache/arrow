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
import os

from ..benchmark.runner import BenchmarkRunner
from ..utils.cmake import CMakeDefinition


def thrutifier(value):
    return "ON" if value else "OFF"


def or_else(value, default):
    return value if value else default


class CppConfiguration:
    def __init__(self,
                 # toolchain
                 cc=None, cxx=None, cxx_flags=None,
                 build_type=None, warn_level=None,
                 # components
                 with_tests=True, with_benchmarks=False, with_python=True,
                 with_parquet=False, with_gandiva=False, with_plasma=False,
                 with_flight=False):
        self.cc = cc
        self.cxx = cxx
        self.cxx_flags = cxx_flags

        self.build_type = build_type
        self.warn_level = warn_level

        self.with_tests = with_tests
        self.with_benchmarks = with_benchmarks
        self.with_python = with_python
        self.with_parquet = with_parquet
        self.with_gandiva = with_gandiva
        self.with_plasma = with_plasma
        self.with_flight = with_flight

    def _gen_defs(self):
        if self.cxx_flags:
            yield ("ARROW_CXXFLAGS", self.cxx_flags)

        yield ("CMAKE_BUILD_TYPE", or_else(self.build_type, "debug"))
        yield ("BUILD_WARNING_LEVEL", or_else(self.warn_level, "production"))

        yield ("ARROW_BUILD_TESTS", thrutifier(self.with_tests))
        yield ("ARROW_BUILD_BENCHMARKS", thrutifier(self.with_benchmarks))

        yield ("ARROW_PYTHON", thrutifier(self.with_python))
        yield ("ARROW_PARQUET", thrutifier(self.with_parquet))
        yield ("ARROW_GANDIVA", thrutifier(self.with_gandiva))
        yield ("ARROW_PLASMA", thrutifier(self.with_plasma))
        yield ("ARROW_FLIGHT", thrutifier(self.with_flight))

    @property
    def definitions(self):
        return [f"-D{d[0]}={d[1]}" for d in self._gen_defs()]

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
