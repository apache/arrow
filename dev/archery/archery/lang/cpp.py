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


def coalesce(value, fallback):
    return fallback if value is None else value


LLVM_VERSION = 7


class CppConfiguration:
    def __init__(self,

                 # toolchain
                 cc=None, cxx=None, cxx_flags=None,
                 build_type=None, warn_level=None,
                 cpp_package_prefix=None, install_prefix=None, use_conda=None,
                 build_static=True, build_shared=True, build_unity=True,
                 # tests & examples
                 with_tests=None, with_benchmarks=None, with_examples=None,
                 with_integration=None,
                 # static checks
                 use_asan=None, use_tsan=None, use_ubsan=None,
                 with_fuzzing=None,
                 # Components
                 with_compute=None, with_csv=None, with_cuda=None,
                 with_dataset=None, with_filesystem=None, with_flight=None,
                 with_gandiva=None, with_gcs=None, with_hdfs=None,
                 with_hiveserver2=None,
                 with_ipc=True, with_json=None,
                 with_mimalloc=None, with_jemalloc=None,
                 with_parquet=None, with_plasma=None, with_python=True,
                 with_r=None, with_s3=None,
                 # Compressions
                 with_brotli=None, with_bz2=None, with_lz4=None,
                 with_snappy=None, with_zlib=None, with_zstd=None,
                 # extras
                 with_lint_only=False,
                 use_gold_linker=True,
                 simd_level="DEFAULT",
                 cmake_extras=None):
        self._cc = cc
        self._cxx = cxx
        self.cxx_flags = cxx_flags

        self._build_type = build_type
        self.warn_level = warn_level
        self._install_prefix = install_prefix
        self._package_prefix = cpp_package_prefix
        self._use_conda = use_conda
        self.build_static = build_static
        self.build_shared = build_shared
        self.build_unity = build_unity

        self.with_tests = with_tests
        self.with_benchmarks = with_benchmarks
        self.with_examples = with_examples
        self.with_integration = with_integration

        self.use_asan = use_asan
        self.use_tsan = use_tsan
        self.use_ubsan = use_ubsan
        self.with_fuzzing = with_fuzzing

        self.with_compute = with_compute
        self.with_csv = with_csv
        self.with_cuda = with_cuda
        self.with_dataset = with_dataset
        self.with_filesystem = with_filesystem
        self.with_flight = with_flight
        self.with_gandiva = with_gandiva
        self.with_gcs = with_gcs
        self.with_hdfs = with_hdfs
        self.with_hiveserver2 = with_hiveserver2
        self.with_ipc = with_ipc
        self.with_json = with_json
        self.with_mimalloc = with_mimalloc
        self.with_jemalloc = with_jemalloc
        self.with_parquet = with_parquet
        self.with_plasma = with_plasma
        self.with_python = with_python
        self.with_r = with_r
        self.with_s3 = with_s3

        self.with_brotli = with_brotli
        self.with_bz2 = with_bz2
        self.with_lz4 = with_lz4
        self.with_snappy = with_snappy
        self.with_zlib = with_zlib
        self.with_zstd = with_zstd

        self.with_lint_only = with_lint_only
        self.use_gold_linker = use_gold_linker
        self.simd_level = simd_level

        self.cmake_extras = cmake_extras

        # Fixup required dependencies by providing sane defaults if the caller
        # didn't specify the option.
        if self.with_r:
            self.with_csv = coalesce(with_csv, True)
            self.with_dataset = coalesce(with_dataset, True)
            self.with_filesystem = coalesce(with_filesystem, True)
            self.with_ipc = coalesce(with_ipc, True)
            self.with_json = coalesce(with_json, True)
            self.with_parquet = coalesce(with_parquet, True)

        if self.with_python:
            self.with_compute = coalesce(with_compute, True)
            self.with_csv = coalesce(with_csv, True)
            self.with_dataset = coalesce(with_dataset, True)
            self.with_filesystem = coalesce(with_filesystem, True)
            self.with_hdfs = coalesce(with_hdfs, True)
            self.with_json = coalesce(with_json, True)
            self.with_lz4 = coalesce(with_lz4, True)
            self.with_zlib = coalesce(with_zlib, True)

        if self.with_dataset:
            self.with_filesystem = coalesce(with_filesystem, True)
            self.with_parquet = coalesce(with_parquet, True)

        if self.with_parquet:
            self.with_snappy = coalesce(with_snappy, True)

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
            return "clang-{}".format(LLVM_VERSION)

        return None

    @property
    def cxx(self):
        if self._cxx:
            return self._cxx

        if self.with_fuzzing:
            return "clang++-{}".format(LLVM_VERSION)

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

        yield ("ARROW_BUILD_STATIC", truthifier(self.build_static))
        yield ("ARROW_BUILD_SHARED", truthifier(self.build_shared))
        yield ("CMAKE_UNITY_BUILD", truthifier(self.build_unity))

        # Tests and benchmarks
        yield ("ARROW_BUILD_TESTS", truthifier(self.with_tests))
        yield ("ARROW_BUILD_BENCHMARKS", truthifier(self.with_benchmarks))
        yield ("ARROW_BUILD_EXAMPLES", truthifier(self.with_examples))
        yield ("ARROW_BUILD_INTEGRATION", truthifier(self.with_integration))

        # Static checks
        yield ("ARROW_USE_ASAN", truthifier(self.use_asan))
        yield ("ARROW_USE_TSAN", truthifier(self.use_tsan))
        yield ("ARROW_USE_UBSAN", truthifier(self.use_ubsan))
        yield ("ARROW_FUZZING", truthifier(self.with_fuzzing))

        # Components
        yield ("ARROW_COMPUTE", truthifier(self.with_compute))
        yield ("ARROW_CSV", truthifier(self.with_csv))
        yield ("ARROW_CUDA", truthifier(self.with_cuda))
        yield ("ARROW_DATASET", truthifier(self.with_dataset))
        yield ("ARROW_FILESYSTEM", truthifier(self.with_filesystem))
        yield ("ARROW_FLIGHT", truthifier(self.with_flight))
        yield ("ARROW_GANDIVA", truthifier(self.with_gandiva))
        yield ("ARROW_GCS", truthifier(self.with_gcs))
        yield ("ARROW_HDFS", truthifier(self.with_hdfs))
        yield ("ARROW_IPC", truthifier(self.with_ipc))
        yield ("ARROW_JSON", truthifier(self.with_json))
        yield ("ARROW_MIMALLOC", truthifier(self.with_mimalloc))
        yield ("ARROW_JEMALLOC", truthifier(self.with_jemalloc))
        yield ("ARROW_PARQUET", truthifier(self.with_parquet))
        yield ("ARROW_PLASMA", truthifier(self.with_plasma))
        yield ("ARROW_S3", truthifier(self.with_s3))

        # Compressions
        yield ("ARROW_WITH_BROTLI", truthifier(self.with_brotli))
        yield ("ARROW_WITH_BZ2", truthifier(self.with_bz2))
        yield ("ARROW_WITH_LZ4", truthifier(self.with_lz4))
        yield ("ARROW_WITH_SNAPPY", truthifier(self.with_snappy))
        yield ("ARROW_WITH_ZLIB", truthifier(self.with_zlib))
        yield ("ARROW_WITH_ZSTD", truthifier(self.with_zstd))

        yield ("ARROW_LINT_ONLY", truthifier(self.with_lint_only))

        # Some configurations don't like gnu gold linker.
        broken_with_gold_ld = [self.with_fuzzing, self.with_gandiva]
        if self.use_gold_linker and not any(broken_with_gold_ld):
            yield ("ARROW_USE_LD_GOLD", truthifier(self.use_gold_linker))
        yield ("ARROW_SIMD_LEVEL", or_else(self.simd_level, "DEFAULT"))

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
        definitions = ["-D{}={}".format(d[0], d[1]) for d in self._gen_defs()]
        return definitions + extras

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
