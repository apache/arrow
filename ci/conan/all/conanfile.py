# MIT License
#
# Copyright (c) 2019 Conan.io
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

from conan import ConanFile
from conan.errors import ConanInvalidConfiguration
from conan.tools.build import check_min_cppstd, cross_building
from conan.tools.cmake import CMake, CMakeDeps, CMakeToolchain, cmake_layout
from conan.tools.files import apply_conandata_patches, copy, export_conandata_patches, get, rmdir
from conan.tools.microsoft import is_msvc, is_msvc_static_runtime
from conan.tools.scm import Version

import os
import glob

required_conan_version = ">=1.53.0"

class ArrowConan(ConanFile):
    name = "arrow"
    description = "Apache Arrow is a cross-language development platform for in-memory data"
    license = ("Apache-2.0",)
    url = "https://github.com/conan-io/conan-center-index"
    homepage = "https://arrow.apache.org/"
    topics = ("memory", "gandiva", "parquet", "skyhook", "acero", "hdfs", "csv", "cuda", "gcs", "json", "hive", "s3", "grpc")
    package_type = "library"
    settings = "os", "arch", "compiler", "build_type"
    options = {
        "shared": [True, False],
        "fPIC": [True, False],
        "gandiva":  [True, False],
        "parquet": ["auto", True, False],
        "substrait": [True, False],
        "skyhook": [True, False],
        "acero": [True, False],
        "cli": [True, False],
        "compute": ["auto", True, False],
        "dataset_modules":  ["auto", True, False],
        "deprecated": [True, False],
        "encryption": [True, False],
        "filesystem_layer":  [True, False],
        "hdfs_bridgs": [True, False],
        "plasma": [True, False, "deprecated"],
        "simd_level": [None, "default", "sse4_2", "avx2", "avx512", "neon", ],
        "runtime_simd_level": [None, "sse4_2", "avx2", "avx512", "max"],
        "with_backtrace": [True, False],
        "with_boost": ["auto", True, False],
        "with_csv": [True, False],
        "with_cuda": [True, False],
        "with_flight_rpc":  ["auto", True, False],
        "with_flight_sql":  [True, False],
        "with_gcs": [True, False],
        "with_gflags": ["auto", True, False],
        "with_glog": ["auto", True, False],
        "with_grpc": ["auto", True, False],
        "with_jemalloc": ["auto", True, False],
        "with_mimalloc": [True, False],
        "with_json": [True, False],
        "with_thrift": ["auto", True, False],
        "with_llvm": ["auto", True, False],
        "with_openssl": ["auto", True, False],
        "with_opentelemetry": [True, False],
        "with_orc": [True, False],
        "with_protobuf": ["auto", True, False],
        "with_re2": ["auto", True, False],
        "with_s3": [True, False],
        "with_utf8proc": ["auto", True, False],
        "with_brotli": [True, False],
        "with_bz2": [True, False],
        "with_lz4": [True, False],
        "with_snappy": [True, False],
        "with_zlib": [True, False],
        "with_zstd": [True, False],
    }
    default_options = {
        "shared": False,
        "fPIC": True,
        "gandiva": False,
        "parquet": "auto",
        "skyhook": False,
        "substrait": False,
        "acero": False,
        "cli": False,
        "compute": "auto",
        "dataset_modules": "auto",
        "deprecated": True,
        "encryption": False,
        "filesystem_layer": False,
        "hdfs_bridgs": False,
        "plasma": "deprecated",
        "simd_level": "default",
        "runtime_simd_level": "max",
        "with_backtrace": False,
        "with_boost": "auto",
        "with_brotli": False,
        "with_bz2": False,
        "with_csv": False,
        "with_cuda": False,
        "with_flight_rpc": "auto",
        "with_flight_sql": False,
        "with_gcs": False,
        "with_gflags": "auto",
        "with_jemalloc": "auto",
        "with_mimalloc": False,
        "with_glog": "auto",
        "with_grpc": "auto",
        "with_json": False,
        "with_thrift": "auto",
        "with_llvm": "auto",
        "with_openssl": "auto",
        "with_opentelemetry": False,
        "with_orc": False,
        "with_protobuf": "auto",
        "with_re2": "auto",
        "with_s3": False,
        "with_utf8proc": "auto",
        "with_lz4": False,
        "with_snappy": False,
        "with_zlib": False,
        "with_zstd": False,
    }
    short_paths = True

    @property
    def _min_cppstd(self):
        # arrow >= 10.0.0 requires C++17.
        # https://github.com/apache/arrow/pull/13991
        return "11" if Version(self.version) < "10.0.0" else "17"

    @property
    def _compilers_minimum_version(self):
        return {
            "11": {
                "clang": "3.9",
            },
            "17": {
                "gcc": "8",
                "clang": "7",
                "apple-clang": "10",
                "Visual Studio": "15",
                "msvc": "191",
            },
        }.get(self._min_cppstd, {})

    def export_sources(self):
        export_conandata_patches(self)
        copy(self, "conan_cmake_project_include.cmake", self.recipe_folder, os.path.join(self.export_sources_folder, "src"))

    def config_options(self):
        if self.settings.os == "Windows":
            del self.options.fPIC
        if Version(self.version) < "2.0.0":
            del self.options.simd_level
            del self.options.runtime_simd_level
        elif Version(self.version) < "6.0.0":
            self.options.simd_level = "sse4_2"
        if Version(self.version) < "6.0.0":
            del self.options.with_gcs
        if Version(self.version) < "7.0.0":
            del self.options.skyhook
            del self.options.with_flight_sql
            del self.options.with_opentelemetry
        if Version(self.version) < "8.0.0":
            del self.options.substrait

    def configure(self):
        if self.options.shared:
            self.options.rm_safe("fPIC")

    def _compute_options(self):
        class ComputedOptions:
            def get_safe(self, option, default=None):
                if hasattr(self, option):
                    return getattr(self, option)
                return default

            def items(self):
                return self.__dict__.items()

        opts = ComputedOptions()
        for k, v in self.options.items():
            if v in ["True", "False"]:
                v = v == "True"
            setattr(opts, k, v)

        # These are only necessary to keep the linter happy
        opts.shared = opts.get_safe("shared", False)
        opts.acero = opts.get_safe("acero", False)
        opts.cli = opts.get_safe("cli", False)
        opts.deprecated = opts.get_safe("deprecated", False)
        opts.encryption = opts.get_safe("encryption", False)
        opts.filesystem_layer = opts.get_safe("filesystem_layer", False)
        opts.gandiva = opts.get_safe("gandiva", False)
        opts.hdfs_bridgs = opts.get_safe("hdfs_bridgs", False)
        opts.plasma = opts.get_safe("plasma", False)
        opts.runtime_simd_level = opts.get_safe("runtime_simd_level", False)
        opts.simd_level = opts.get_safe("simd_level", False)
        opts.substrait = opts.get_safe("substrait", False)
        opts.with_backtrace = opts.get_safe("with_backtrace", False)
        opts.with_brotli = opts.get_safe("with_brotli", False)
        opts.with_bz2 = opts.get_safe("with_bz2", False)
        opts.with_csv = opts.get_safe("with_csv", False)
        opts.with_cuda = opts.get_safe("with_cuda", False)
        opts.with_lz4 = opts.get_safe("with_lz4", False)
        opts.with_mimalloc = opts.get_safe("with_mimalloc", False)
        opts.with_orc = opts.get_safe("with_orc", False)
        opts.with_s3 = opts.get_safe("with_s3", False)
        opts.with_snappy = opts.get_safe("with_snappy", False)
        opts.with_zlib = opts.get_safe("with_zlib", False)
        opts.with_zstd = opts.get_safe("with_zstd", False)

        if opts.parquet == "auto":
            opts.parquet = opts.substrait
        if opts.dataset_modules == "auto":
            opts.dataset_modules = opts.substrait
        if opts.compute == "auto":
            opts.compute = opts.parquet or opts.dataset_modules or opts.substrait
        if opts.with_boost == "auto":
            if opts.gandiva:
                opts.with_boost = True
            elif Version(self.version).major == 1:
                opts.with_boost = opts.parquet and self.settings.compiler == "gcc" and self.settings.compiler.version < Version("4.9")
            else:
                opts.with_boost = is_msvc(self)
        if opts.with_flight_rpc == "auto":
            opts.with_flight_rpc = opts.get_safe("with_flight_sql", False)
        if opts.with_glog == "auto":
            opts.with_glog = False
        if opts.with_grpc == "auto":
            opts.with_grpc = opts.with_flight_rpc
        if opts.with_gflags == "auto":
            opts.with_gflags = opts.with_glog or opts.with_grpc
        if opts.with_jemalloc == "auto":
            opts.with_jemalloc = "BSD" in str(self.settings.os)
        if opts.with_thrift == "auto":
            opts.with_thrift = opts.parquet
        if opts.with_llvm == "auto":
            opts.with_llvm = opts.gandiva
        if opts.with_openssl == "auto":
            opts.with_openssl = bool(opts.encryption or opts.with_flight_rpc or opts.with_s3)
        if opts.with_protobuf == "auto":
            opts.with_protobuf = opts.gandiva or opts.with_flight_rpc or opts.with_orc or opts.substrait
        if opts.with_re2 == "auto":
            opts.with_re2 = opts.gandiva or opts.parquet or (
                    Version(self) >= "7.0.0" and (opts.compute or opts.dataset_modules))
        if opts.with_utf8proc == "auto":
            opts.with_utf8proc = opts.compute or opts.gandiva
        if Version(self.version) >= "7.0.0" and opts.encryption:
            opts.with_json = True

        return opts

    def layout(self):
        cmake_layout(self, src_folder="src")

    def requirements(self):
        self._opts = self._compute_options()

        if self._opts.with_thrift:
            self.requires("thrift/0.17.0")
        if self._opts.with_protobuf:
            self.requires("protobuf/3.21.12")
        if self._opts.with_jemalloc:
            self.requires("jemalloc/5.3.0")
        if self._opts.with_mimalloc:
            self.requires("mimalloc/1.7.6")
        if self._opts.with_boost:
            self.requires("boost/1.83.0")
        if self._opts.with_gflags:
            self.requires("gflags/2.2.2")
        if self._opts.with_glog:
            self.requires("glog/0.6.0")
        if self._opts.get_safe("with_gcs"):
            self.requires("google-cloud-cpp/1.40.1")
        if self._opts.with_grpc:
            self.requires("grpc/1.50.0")
        if self._opts.with_json:
            self.requires("rapidjson/1.1.0")
        if self._opts.with_llvm:
            self.requires("llvm-core/13.0.0")
        if self._opts.with_openssl:
            # aws-sdk-cpp requires openssl/1.1.1. it uses deprecated functions in openssl/3.0.0
            if self._opts.with_s3:
                self.requires("openssl/1.1.1w")
            else:
                self.requires("openssl/[>=1.1 <4]")
        if self._opts.get_safe("with_opentelemetry"):
            self.requires("opentelemetry-cpp/1.7.0")
        if self._opts.with_s3:
            self.requires("aws-sdk-cpp/1.9.234")
        if self._opts.with_brotli:
            self.requires("brotli/1.1.0")
        if self._opts.with_bz2:
            self.requires("bzip2/1.0.8")
        if self._opts.with_lz4:
            self.requires("lz4/1.9.4")
        if self._opts.with_snappy:
            self.requires("snappy/1.1.9")
        if Version(self.version) >= "6.0.0" and \
            self._opts.get_safe("simd_level") != None or \
            self._opts.get_safe("runtime_simd_level") != None:
            self.requires("xsimd/9.0.1")
        if self._opts.with_zlib:
            self.requires("zlib/[>=1.2.11 <2]")
        if self._opts.with_zstd:
            self.requires("zstd/1.5.5")
        if self._opts.with_re2:
            self.requires("re2/20230301")
        if self._opts.with_utf8proc:
            self.requires("utf8proc/2.8.0")
        if self._opts.with_backtrace:
            self.requires("libbacktrace/cci.20210118")

    def package_id(self):
        for option, _ in self.info.options.items():
            setattr(self.info.options, option, self._opts.get_safe(option, False))

    def validate(self):
        for option, value in self._opts.items():
            assert value != "auto", f"Option '{option}' contains 'auto' value, which is not allowed. There is a bug in the recipe."

        if self.settings.compiler.get_safe("cppstd"):
            check_min_cppstd(self, self._min_cppstd)

        minimum_version = self._compilers_minimum_version.get(str(self.settings.compiler), False)
        if minimum_version and Version(self.settings.compiler.version) < minimum_version:
            raise ConanInvalidConfiguration(
                f"{self.ref} requires C++{self._min_cppstd}, which your compiler does not support."
            )

        if self._opts.get_safe("skyhook", False):
            raise ConanInvalidConfiguration("CCI has no librados recipe (yet)")
        if self._opts.with_cuda:
            raise ConanInvalidConfiguration("CCI has no cuda recipe (yet)")
        if self._opts.with_orc:
            raise ConanInvalidConfiguration("CCI has no orc recipe (yet)")
        if self._opts.with_s3 and not self.dependencies["aws-sdk-cpp"].options.config:
            raise ConanInvalidConfiguration("arrow:with_s3 requires aws-sdk-cpp:config is True.")

        if self._opts.shared and self._opts.with_jemalloc:
            if self.dependencies["jemalloc"].options.enable_cxx:
                raise ConanInvalidConfiguration("jemmalloc.enable_cxx of a static jemalloc must be disabled")

        if Version(self.version) < "6.0.0" and self._opts.get_safe("simd_level") == "default":
            raise ConanInvalidConfiguration(f"In {self.ref}, simd_level options is not supported `default` value.")

    def build_requirements(self):
        if Version(self.version) >= "13.0.0":
            self.tool_requires("cmake/[>=3.16 <4]")

    def source(self):
        # START
        # This block should be removed when we update upstream:
        # https://github.com/conan-io/conan-center-index/tree/master/recipes/arrow/
        if not self.version in self.conan_data.get("sources", {}):
            import shutil
            top_level = os.environ.get("ARROW_HOME")
            shutil.copytree(os.path.join(top_level, "cpp"),
                            os.path.join(self.source_folder, "cpp"))
            shutil.copytree(os.path.join(top_level, "format"),
                            os.path.join(self.source_folder, "format"))
            top_level_files = [
                ".env",
                "LICENSE.txt",
                "NOTICE.txt",
            ]
            for top_level_file in top_level_files:
                shutil.copy(os.path.join(top_level, top_level_file),
                            self.source_folder)
            return
        # END
        get(self, **self.conan_data["sources"][self.version],
            filename=f"apache-arrow-{self.version}.tar.gz", strip_root=True)

    def generate(self):
        tc = CMakeToolchain(self)
        if cross_building(self):
            cmake_system_processor = {
                "armv8": "aarch64",
                "armv8.3": "aarch64",
            }.get(str(self.settings.arch), str(self.settings.arch))
            if cmake_system_processor == "aarch64":
                tc.variables["ARROW_CPU_FLAG"] = "armv8"
        if is_msvc(self):
            tc.variables["ARROW_USE_STATIC_CRT"] = is_msvc_static_runtime(self)
        tc.variables["ARROW_DEPENDENCY_SOURCE"] = "SYSTEM"
        tc.variables["ARROW_PACKAGE_KIND"] = "conan" # See https://github.com/conan-io/conan-center-index/pull/14903/files#r1057938314 for details
        tc.variables["ARROW_GANDIVA"] = self._opts.gandiva
        tc.variables["ARROW_PARQUET"] = self._opts.parquet
        tc.variables["ARROW_SUBSTRAIT"] = self._opts.get_safe("substrait", False)
        tc.variables["ARROW_ACERO"] = self._opts.acero
        tc.variables["ARROW_DATASET"] = self._opts.dataset_modules
        tc.variables["ARROW_FILESYSTEM"] = self._opts.filesystem_layer
        tc.variables["PARQUET_REQUIRE_ENCRYPTION"] = self._opts.encryption
        tc.variables["ARROW_HDFS"] = self._opts.hdfs_bridgs
        tc.variables["ARROW_VERBOSE_THIRDPARTY_BUILD"] = True
        tc.variables["ARROW_BUILD_SHARED"] = self._opts.shared
        tc.variables["ARROW_BUILD_STATIC"] = not self._opts.shared
        tc.variables["ARROW_NO_DEPRECATED_API"] = not self._opts.deprecated
        tc.variables["ARROW_FLIGHT"] = self._opts.with_flight_rpc
        tc.variables["ARROW_FLIGHT_SQL"] = self._opts.get_safe("with_flight_sql", False)
        tc.variables["ARROW_COMPUTE"] = self._opts.compute
        tc.variables["ARROW_CSV"] = self._opts.with_csv
        tc.variables["ARROW_CUDA"] = self._opts.with_cuda
        tc.variables["ARROW_JEMALLOC"] = self._opts.with_jemalloc
        tc.variables["jemalloc_SOURCE"] = "SYSTEM"
        tc.variables["ARROW_MIMALLOC"] = self._opts.with_mimalloc
        tc.variables["ARROW_JSON"] = self._opts.with_json
        tc.variables["google_cloud_cpp_SOURCE"] = "SYSTEM"
        tc.variables["ARROW_GCS"] = self._opts.get_safe("with_gcs", False)
        tc.variables["BOOST_SOURCE"] = "SYSTEM"
        tc.variables["Protobuf_SOURCE"] = "SYSTEM"
        if self._opts.with_protobuf:
            tc.variables["ARROW_PROTOBUF_USE_SHARED"] = self.dependencies["protobuf"].options.shared
        tc.variables["gRPC_SOURCE"] = "SYSTEM"
        if self._opts.with_grpc:
            tc.variables["ARROW_GRPC_USE_SHARED"] = self.dependencies["grpc"].options.shared

        tc.variables["ARROW_USE_GLOG"] = self._opts.with_glog
        tc.variables["GLOG_SOURCE"] = "SYSTEM"
        tc.variables["ARROW_WITH_BACKTRACE"] = self._opts.with_backtrace
        tc.variables["ARROW_WITH_BROTLI"] = self._opts.with_brotli
        tc.variables["brotli_SOURCE"] = "SYSTEM"
        if self._opts.with_brotli:
            tc.variables["ARROW_BROTLI_USE_SHARED"] = self.dependencies["brotli"].options.shared
        tc.variables["gflags_SOURCE"] = "SYSTEM"
        if self._opts.with_gflags:
            tc.variables["ARROW_GFLAGS_USE_SHARED"] = self.dependencies["gflags"].options.shared
        tc.variables["ARROW_WITH_BZ2"] = self._opts.with_bz2
        tc.variables["BZip2_SOURCE"] = "SYSTEM"
        if self._opts.with_bz2:
            tc.variables["ARROW_BZ2_USE_SHARED"] = self.dependencies["bzip2"].options.shared
        tc.variables["ARROW_WITH_LZ4"] = self._opts.with_lz4
        tc.variables["lz4_SOURCE"] = "SYSTEM"
        if self._opts.with_lz4:
            tc.variables["ARROW_LZ4_USE_SHARED"] = self.dependencies["lz4"].options.shared
        tc.variables["ARROW_WITH_SNAPPY"] = self._opts.with_snappy
        tc.variables["RapidJSON_SOURCE"] = "SYSTEM"
        tc.variables["Snappy_SOURCE"] = "SYSTEM"
        if self._opts.with_snappy:
            tc.variables["ARROW_SNAPPY_USE_SHARED"] = self.dependencies["snappy"].options.shared
        tc.variables["ARROW_WITH_ZLIB"] = self._opts.with_zlib
        tc.variables["re2_SOURCE"] = "SYSTEM"
        tc.variables["ZLIB_SOURCE"] = "SYSTEM"
        tc.variables["xsimd_SOURCE"] = "SYSTEM"
        tc.variables["ARROW_WITH_ZSTD"] = self._opts.with_zstd
        if Version(self.version) >= "2.0":
            tc.variables["zstd_SOURCE"] = "SYSTEM"
            tc.variables["ARROW_SIMD_LEVEL"] = str(self._opts.simd_level).upper()
            tc.variables["ARROW_RUNTIME_SIMD_LEVEL"] = str(self._opts.runtime_simd_level).upper()
        else:
            tc.variables["ZSTD_SOURCE"] = "SYSTEM"
        if self._opts.with_zstd:
            tc.variables["ARROW_ZSTD_USE_SHARED"] = self.dependencies["zstd"].options.shared
        tc.variables["ORC_SOURCE"] = "SYSTEM"
        tc.variables["ARROW_WITH_THRIFT"] = self._opts.with_thrift
        tc.variables["Thrift_SOURCE"] = "SYSTEM"
        if self._opts.with_thrift:
            tc.variables["THRIFT_VERSION"] = self.dependencies["thrift"].ref.version # a recent thrift does not require boost
            tc.variables["ARROW_THRIFT_USE_SHARED"] = self.dependencies["thrift"].options.shared
        tc.variables["ARROW_USE_OPENSSL"] = self._opts.with_openssl
        if self._opts.with_openssl:
            tc.variables["OPENSSL_ROOT_DIR"] = self.dependencies["openssl"].package_folder.replace("\\", "/")
            tc.variables["ARROW_OPENSSL_USE_SHARED"] = self.dependencies["openssl"].options.shared
        if self._opts.with_boost:
            tc.variables["ARROW_USE_BOOST"] = True
            tc.variables["ARROW_BOOST_USE_SHARED"] = self.dependencies["boost"].options.shared
        tc.variables["ARROW_S3"] = self._opts.with_s3
        tc.variables["AWSSDK_SOURCE"] = "SYSTEM"
        tc.variables["ARROW_BUILD_UTILITIES"] = self._opts.cli
        tc.variables["ARROW_BUILD_INTEGRATION"] = False
        tc.variables["ARROW_INSTALL_NAME_RPATH"] = True
        tc.variables["ARROW_BUILD_EXAMPLES"] = False
        tc.variables["ARROW_BUILD_TESTS"] = False
        tc.variables["ARROW_ENABLE_TIMING_TESTS"] = False
        tc.variables["ARROW_BUILD_BENCHMARKS"] = False
        tc.variables["LLVM_SOURCE"] = "SYSTEM"
        tc.variables["ARROW_WITH_UTF8PROC"] = self._opts.with_utf8proc
        tc.variables["ARROW_BOOST_REQUIRED"] = self._opts.with_boost
        tc.variables["utf8proc_SOURCE"] = "SYSTEM"
        if self._opts.with_utf8proc:
            tc.variables["ARROW_UTF8PROC_USE_SHARED"] = self.dependencies["utf8proc"].options.shared
        tc.variables["BUILD_WARNING_LEVEL"] = "PRODUCTION"
        if is_msvc(self):
            tc.variables["ARROW_USE_STATIC_CRT"] = is_msvc_static_runtime(self)
        if self._opts.with_llvm:
            tc.variables["LLVM_DIR"] = self.dependencies["llvm-core"].package_folder.replace("\\", "/")

        tc.cache_variables["CMAKE_PROJECT_arrow_INCLUDE"] = os.path.join(self.source_folder, "conan_cmake_project_include.cmake")
        tc.generate()

        deps = CMakeDeps(self)
        deps.generate()

    def _patch_sources(self):
        apply_conandata_patches(self)
        if "7.0.0" <= Version(self.version) < "10.0.0":
            for filename in glob.glob(os.path.join(self.source_folder, "cpp", "cmake_modules", "Find*.cmake")):
                if os.path.basename(filename) not in [
                    "FindArrow.cmake",
                    "FindArrowAcero.cmake",
                    "FindArrowCUDA.cmake",
                    "FindArrowDataset.cmake",
                    "FindArrowFlight.cmake",
                    "FindArrowFlightSql.cmake",
                    "FindArrowFlightTesting.cmake",
                    "FindArrowPython.cmake",
                    "FindArrowPythonFlight.cmake",
                    "FindArrowSubstrait.cmake",
                    "FindArrowTesting.cmake",
                    "FindGandiva.cmake",
                    "FindParquet.cmake",
                ]:
                    os.remove(filename)

    def build(self):
        self._patch_sources()
        cmake = CMake(self)
        cmake.configure(build_script_folder=os.path.join(self.source_folder, "cpp"))
        cmake.build()

    def package(self):
        copy(self, pattern="LICENSE.txt", dst=os.path.join(self.package_folder, "licenses"), src=self.source_folder)
        copy(self, pattern="NOTICE.txt", dst=os.path.join(self.package_folder, "licenses"), src=self.source_folder)
        cmake = CMake(self)
        cmake.install()

        rmdir(self, os.path.join(self.package_folder, "lib", "cmake"))
        rmdir(self, os.path.join(self.package_folder, "lib", "pkgconfig"))
        rmdir(self, os.path.join(self.package_folder, "share"))

    def package_info(self):
        # FIXME: fix CMake targets of components

        self.cpp_info.set_property("cmake_file_name", "Arrow")

        suffix = "_static" if is_msvc(self) and not self._opts.shared else ""

        self.cpp_info.components["libarrow"].set_property("pkg_config_name", "arrow")
        self.cpp_info.components["libarrow"].libs = [f"arrow{suffix}"]
        if not self._opts.shared:
            self.cpp_info.components["libarrow"].defines = ["ARROW_STATIC"]
            if self.settings.os in ["Linux", "FreeBSD"]:
                self.cpp_info.components["libarrow"].system_libs = ["pthread", "m", "dl", "rt"]

        if self._opts.parquet:
            self.cpp_info.components["libparquet"].set_property("pkg_config_name", "parquet")
            self.cpp_info.components["libparquet"].libs = [f"parquet{suffix}"]
            self.cpp_info.components["libparquet"].requires = ["libarrow"]
            if not self._opts.shared:
                self.cpp_info.components["libparquet"].defines = ["PARQUET_STATIC"]

        if self._opts.get_safe("substrait"):
            self.cpp_info.components["libarrow_substrait"].set_property("pkg_config_name", "arrow_substrait")
            self.cpp_info.components["libarrow_substrait"].libs = [f"arrow_substrait{suffix}"]
            self.cpp_info.components["libarrow_substrait"].requires = ["libparquet", "dataset"]

        # Plasma was deprecated in Arrow 12.0.0
        del self._opts.plasma

        if self._opts.acero:
            self.cpp_info.components["libacero"].libs = [f"arrow_acero{suffix}"]
            self.cpp_info.components["libacero"].names["cmake_find_package"] = "acero"
            self.cpp_info.components["libacero"].names["cmake_find_package_multi"] = "acero"
            self.cpp_info.components["libacero"].names["pkg_config"] = "acero"
            self.cpp_info.components["libacero"].requires = ["libarrow"]

        if self._opts.gandiva:
            self.cpp_info.components["libgandiva"].set_property("pkg_config_name", "gandiva")
            self.cpp_info.components["libgandiva"].libs = [f"gandiva{suffix}"]
            self.cpp_info.components["libgandiva"].requires = ["libarrow"]
            if not self._opts.shared:
                self.cpp_info.components["libgandiva"].defines = ["GANDIVA_STATIC"]

        if self._opts.with_flight_rpc:
            self.cpp_info.components["libarrow_flight"].set_property("pkg_config_name", "flight_rpc")
            self.cpp_info.components["libarrow_flight"].libs = [f"arrow_flight{suffix}"]
            self.cpp_info.components["libarrow_flight"].requires = ["libarrow"]

        if self._opts.get_safe("with_flight_sql"):
            self.cpp_info.components["libarrow_flight_sql"].set_property("pkg_config_name", "flight_sql")
            self.cpp_info.components["libarrow_flight_sql"].libs = [f"arrow_flight_sql{suffix}"]
            self.cpp_info.components["libarrow_flight_sql"].requires = ["libarrow", "libarrow_flight"]

        if self._opts.dataset_modules:
            self.cpp_info.components["dataset"].libs = ["arrow_dataset"]
            if self._opts.parquet:
                self.cpp_info.components["dataset"].requires = ["libparquet"]

        if self._opts.cli and (self._opts.with_cuda or self._opts.with_flight_rpc or self._opts.parquet):
            binpath = os.path.join(self.package_folder, "bin")
            self.output.info(f"Appending PATH env var: {binpath}")
            self.env_info.PATH.append(binpath)

        if self._opts.with_boost:
            if self._opts.gandiva:
                # FIXME: only filesystem component is used
                self.cpp_info.components["libgandiva"].requires.append("boost::boost")
            if self._opts.parquet and self.settings.compiler == "gcc" and self.settings.compiler.version < Version("4.9"):
                self.cpp_info.components["libparquet"].requires.append("boost::boost")
            if Version(self.version) >= "2.0":
                # FIXME: only headers components is used
                self.cpp_info.components["libarrow"].requires.append("boost::boost")
        if self._opts.with_openssl:
            self.cpp_info.components["libarrow"].requires.append("openssl::openssl")
        if self._opts.with_gflags:
            self.cpp_info.components["libarrow"].requires.append("gflags::gflags")
        if self._opts.with_glog:
            self.cpp_info.components["libarrow"].requires.append("glog::glog")
        if self._opts.with_jemalloc:
            self.cpp_info.components["libarrow"].requires.append("jemalloc::jemalloc")
        if self._opts.with_mimalloc:
            self.cpp_info.components["libarrow"].requires.append("mimalloc::mimalloc")
        if self._opts.with_re2:
            if self._opts.gandiva:
                self.cpp_info.components["libgandiva"].requires.append("re2::re2")
            if self._opts.parquet:
                self.cpp_info.components["libparquet"].requires.append("re2::re2")
            self.cpp_info.components["libarrow"].requires.append("re2::re2")
        if self._opts.with_llvm:
            self.cpp_info.components["libgandiva"].requires.append("llvm-core::llvm-core")
        if self._opts.with_protobuf:
            self.cpp_info.components["libarrow"].requires.append("protobuf::protobuf")
        if self._opts.with_utf8proc:
            self.cpp_info.components["libarrow"].requires.append("utf8proc::utf8proc")
        if self._opts.with_thrift:
            self.cpp_info.components["libarrow"].requires.append("thrift::thrift")
        if self._opts.with_backtrace:
            self.cpp_info.components["libarrow"].requires.append("libbacktrace::libbacktrace")
        if self._opts.with_cuda:
            self.cpp_info.components["libarrow"].requires.append("cuda::cuda")
        if self._opts.with_json:
            self.cpp_info.components["libarrow"].requires.append("rapidjson::rapidjson")
        if self._opts.with_s3:
            self.cpp_info.components["libarrow"].requires.append("aws-sdk-cpp::s3")
        if self._opts.get_safe("with_gcs"):
            self.cpp_info.components["libarrow"].requires.append("google-cloud-cpp::storage")
        if self._opts.with_orc:
            self.cpp_info.components["libarrow"].requires.append("orc::orc")
        if self._opts.get_safe("with_opentelemetry"):
            self.cpp_info.components["libarrow"].requires.append("opentelemetry-cpp::opentelemetry-cpp")
        if self._opts.with_brotli:
            self.cpp_info.components["libarrow"].requires.append("brotli::brotli")
        if self._opts.with_bz2:
            self.cpp_info.components["libarrow"].requires.append("bzip2::bzip2")
        if self._opts.with_lz4:
            self.cpp_info.components["libarrow"].requires.append("lz4::lz4")
        if self._opts.with_snappy:
            self.cpp_info.components["libarrow"].requires.append("snappy::snappy")
        if self._opts.get_safe("simd_level") != None or self._opts.get_safe("runtime_simd_level") != None:
            self.cpp_info.components["libarrow"].requires.append("xsimd::xsimd")
        if self._opts.with_zlib:
            self.cpp_info.components["libarrow"].requires.append("zlib::zlib")
        if self._opts.with_zstd:
            self.cpp_info.components["libarrow"].requires.append("zstd::zstd")
        if self._opts.with_boost:
            self.cpp_info.components["libarrow"].requires.append("boost::boost")
        if self._opts.with_grpc:
            self.cpp_info.components["libarrow"].requires.append("grpc::grpc")
        if self._opts.with_flight_rpc:
            self.cpp_info.components["libarrow_flight"].requires.append("protobuf::protobuf")

        # TODO: to remove in conan v2
        self.cpp_info.filenames["cmake_find_package"] = "Arrow"
        self.cpp_info.filenames["cmake_find_package_multi"] = "Arrow"
        self.cpp_info.components["libarrow"].names["cmake_find_package"] = "arrow"
        self.cpp_info.components["libarrow"].names["cmake_find_package_multi"] = "arrow"
        if self._opts.parquet:
            self.cpp_info.components["libparquet"].names["cmake_find_package"] = "parquet"
            self.cpp_info.components["libparquet"].names["cmake_find_package_multi"] = "parquet"
        if self._opts.get_safe("substrait"):
            self.cpp_info.components["libarrow_substrait"].names["cmake_find_package"] = "arrow_substrait"
            self.cpp_info.components["libarrow_substrait"].names["cmake_find_package_multi"] = "arrow_substrait"
        if self._opts.gandiva:
            self.cpp_info.components["libgandiva"].names["cmake_find_package"] = "gandiva"
            self.cpp_info.components["libgandiva"].names["cmake_find_package_multi"] = "gandiva"
        if self._opts.with_flight_rpc:
            self.cpp_info.components["libarrow_flight"].names["cmake_find_package"] = "flight_rpc"
            self.cpp_info.components["libarrow_flight"].names["cmake_find_package_multi"] = "flight_rpc"
        if self._opts.get_safe("with_flight_sql"):
            self.cpp_info.components["libarrow_flight_sql"].names["cmake_find_package"] = "flight_sql"
            self.cpp_info.components["libarrow_flight_sql"].names["cmake_find_package_multi"] = "flight_sql"
        if self._opts.cli and (self._opts.with_cuda or self._opts.with_flight_rpc or self._opts.parquet):
            self.env_info.PATH.append(os.path.join(self.package_folder, "bin"))
