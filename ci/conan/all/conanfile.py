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
from conan.tools.microsoft import is_msvc_static_runtime, is_msvc, check_min_vs
from conan.tools.files import export_conandata_patches, apply_conandata_patches, get, copy, rmdir
from conan.tools.build import check_min_cppstd, cross_building
from conan.tools.scm import Version
from conan.tools.cmake import CMake, CMakeDeps, CMakeToolchain, cmake_layout

import os
import glob

required_conan_version = ">=1.53.0"

class ArrowConan(ConanFile):
    name = "arrow"
    description = "Apache Arrow is a cross-language development platform for in-memory data"
    license = ("Apache-2.0",)
    url = "https://github.com/conan-io/conan-center-index"
    homepage = "https://arrow.apache.org/"
    topics = ("memory", "gandiva", "parquet", "skyhook", "plasma", "hdfs", "csv", "cuda", "gcs", "json", "hive", "s3", "grpc")
    settings = "os", "arch", "compiler", "build_type"
    options = {
        "shared": [True, False],
        "fPIC": [True, False],
        "gandiva":  [True, False],
        "parquet": ["auto", True, False],
        "substrait": [True, False],
        "skyhook": [True, False],
        "plasma": [True, False],
        "cli": [True, False],
        "compute": ["auto", True, False],
        "dataset_modules":  ["auto", True, False],
        "deprecated": [True, False],
        "encryption": [True, False],
        "filesystem_layer":  [True, False],
        "hdfs_bridgs": [True, False],
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
        "with_mimalloc": ["auto", True, False],
        "with_json": [True, False],
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
        "plasma": False,
        "cli": False,
        "compute": "auto",
        "dataset_modules": "auto",
        "deprecated": True,
        "encryption": False,
        "filesystem_layer": False,
        "hdfs_bridgs": False,
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
    def _minimum_cpp_standard(self):
        # arrow >= 10.0.0 requires C++17.
        # https://github.com/apache/arrow/pull/13991
        return 11 if Version(self.version) < "10.0.0" else 17

    @property
    def _compilers_minimum_version(self):
        return {
            "gcc": "8",
            "clang": "7",
            "apple-clang": "10",
        }

    def export_sources(self):
        export_conandata_patches(self)

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

    def validate(self):
        if self.info.settings.compiler.cppstd:
            check_min_cppstd(self, self._minimum_cpp_standard)

        if self._minimum_cpp_standard == 11:
            if self.info.settings.compiler == "clang" and self.info.settings.compiler.version <= Version("3.9"):
                raise ConanInvalidConfiguration("This recipe does not support this compiler version")
        else:
            check_min_vs(self, 191)
            if not is_msvc(self):
                minimum_version = self._compilers_minimum_version.get(str(self.info.settings.compiler), False)
                if minimum_version and Version(self.info.settings.compiler.version) < minimum_version:
                    raise ConanInvalidConfiguration(
                        f"{self.ref} requires C++{self._minimum_cpp_standard}, which your compiler does not support."
                    )

        if self.options.shared:
            del self.options.fPIC
        if self.options.compute == False and not self._compute(True):
            raise ConanInvalidConfiguration("compute options is required (or choose auto)")
        if self.options.parquet == False and self._parquet(True):
            raise ConanInvalidConfiguration("parquet options is required (or choose auto)")
        if self.options.dataset_modules == False and self._dataset_modules(True):
            raise ConanInvalidConfiguration("dataset_modules options is required (or choose auto)")
        if self.options.get_safe("skyhook", False):
            raise ConanInvalidConfiguration("CCI has no librados recipe (yet)")
        if self.options.with_jemalloc == False and self._with_jemalloc(True):
            raise ConanInvalidConfiguration("with_jemalloc option is required (or choose auto)")
        if self.options.with_re2 == False and self._with_re2(True):
            raise ConanInvalidConfiguration("with_re2 option is required (or choose auto)")
        if self.options.with_protobuf == False and self._with_protobuf(True):
            raise ConanInvalidConfiguration("with_protobuf option is required (or choose auto)")
        if self.options.with_gflags == False and self._with_gflags(True):
            raise ConanInvalidConfiguration("with_gflags options is required (or choose auto)")
        if self.options.with_flight_rpc == False and self._with_flight_rpc(True):
            raise ConanInvalidConfiguration("with_flight_rpc options is required (or choose auto)")
        if self.options.with_grpc == False and self._with_grpc(True):
            raise ConanInvalidConfiguration("with_grpc options is required (or choose auto)")
        if self.options.with_boost == False and self._with_boost(True):
            raise ConanInvalidConfiguration("with_boost options is required (or choose auto)")
        if self.options.with_openssl == False and self._with_openssl(True):
            raise ConanInvalidConfiguration("with_openssl options is required (or choose auto)")
        if self.options.with_llvm == False and self._with_llvm(True):
            raise ConanInvalidConfiguration("with_llvm options is required (or choose auto)")
        if self.options.with_cuda:
            raise ConanInvalidConfiguration("CCI has no cuda recipe (yet)")
        if self.options.with_orc:
            raise ConanInvalidConfiguration("CCI has no orc recipe (yet)")
        if self.options.with_s3 and not self.options["aws-sdk-cpp"].config:
            raise ConanInvalidConfiguration("arrow:with_s3 requires aws-sdk-cpp:config is True.")

        if self.options.shared and self._with_jemalloc():
            if self.options["jemalloc"].enable_cxx:
                raise ConanInvalidConfiguration("jemmalloc.enable_cxx of a static jemalloc must be disabled")

        if Version(self.version) < "6.0.0" and self.options.get_safe("simd_level") == "default":
            raise ConanInvalidConfiguration(f"In {self.ref}, simd_level options is not supported `default` value.")

    def layout(self):
        cmake_layout(self, src_folder="src")

    def _compute(self, required=False):
        if required or self.options.compute == "auto":
            return bool(self._parquet()) or bool(self._dataset_modules()) or bool(self.options.get_safe("substrait", False))
        else:
            return bool(self.options.compute)

    def _parquet(self, required=False):
        if required or self.options.parquet == "auto":
            return bool(self.options.get_safe("substrait", False))
        else:
            return bool(self.options.parquet)

    def _dataset_modules(self, required=False):
        if required or self.options.dataset_modules == "auto":
            return bool(self.options.get_safe("substrait", False))
        else:
            return bool(self.options.dataset_modules)

    def _with_jemalloc(self, required=False):
        if required or self.options.with_jemalloc == "auto":
            return bool("BSD" in str(self.settings.os))
        else:
            return bool(self.options.with_jemalloc)

    def _with_re2(self, required=False):
        if required or self.options.with_re2 == "auto":
            if self.options.gandiva or self.options.parquet:
                return True
            if Version(self) >= "7.0.0" and (self._compute() or self._dataset_modules()):
                return True
            return False
        else:
            return bool(self.options.with_re2)

    def _with_protobuf(self, required=False):
        if required or self.options.with_protobuf == "auto":
            return bool(self.options.gandiva or self._with_flight_rpc() or self.options.with_orc or self.options.get_safe("substrait", False))
        else:
            return bool(self.options.with_protobuf)

    def _with_flight_rpc(self, required=False):
        if required or self.options.with_flight_rpc == "auto":
            return bool(self.options.get_safe("with_flight_sql", False))
        else:
            return bool(self.options.with_flight_rpc)

    def _with_gflags(self, required=False):
        if required or self.options.with_gflags == "auto":
            return bool(self.options.plasma or self._with_glog() or self._with_grpc())
        else:
            return bool(self.options.with_gflags)

    def _with_glog(self, required=False):
        if required or self.options.with_glog == "auto":
            return False
        else:
            return bool(self.options.with_glog)

    def _with_grpc(self, required=False):
        if required or self.options.with_grpc == "auto":
            return self._with_flight_rpc()
        else:
            return bool(self.options.with_grpc)

    def _with_boost(self, required=False):
        if required or self.options.with_boost == "auto":
            if self.options.gandiva:
                return True
            version = Version(self.version)
            if version.major == "1":
                if self._parquet() and self.settings.compiler == "gcc" and self.settings.compiler.version < Version("4.9"):
                    return True
            elif version.major >= "2":
                if is_msvc(self):
                    return True
            return False
        else:
            return bool(self.options.with_boost)

    def _with_thrift(self, required=False):
        # No self.options.with_thift exists
        return bool(required or self._parquet())

    def _with_utf8proc(self, required=False):
        if required or self.options.with_utf8proc == "auto":
            return bool(self._compute() or self.options.gandiva)
        else:
            return bool(self.options.with_utf8proc)

    def _with_llvm(self, required=False):
        if required or self.options.with_llvm == "auto":
            return bool(self.options.gandiva)
        else:
            return bool(self.options.with_llvm)

    def _with_openssl(self, required=False):
        if required or self.options.with_openssl == "auto":
            return bool(self.options.encryption or self._with_flight_rpc() or self.options.with_s3)
        else:
            return bool(self.options.with_openssl)

    def _with_rapidjson(self):
        if self.options.with_json:
            return True
        if Version(self.version) >= "7.0.0" and self.options.encryption:
            return True
        return False

    def requirements(self):
        if self._with_thrift():
            self.requires("thrift/0.17.0")
        if self._with_protobuf():
            self.requires("protobuf/3.21.4")
        if self._with_jemalloc():
            self.requires("jemalloc/5.3.0")
        if self.options.with_mimalloc:
            self.requires("mimalloc/1.7.6")
        if self._with_boost():
            self.requires("boost/1.80.0")
        if self._with_gflags():
            self.requires("gflags/2.2.2")
        if self._with_glog():
            self.requires("glog/0.6.0")
        if self.options.get_safe("with_gcs"):
            self.requires("google-cloud-cpp/1.40.1")
        if self._with_grpc():
            self.requires("grpc/1.50.0")
        if self._with_rapidjson():
            self.requires("rapidjson/1.1.0")
        if self._with_llvm():
            self.requires("llvm-core/13.0.0")
        if self._with_openssl():
            # aws-sdk-cpp requires openssl/1.1.1. it uses deprecated functions in openssl/3.0.0
            if self.options.with_s3:
                self.requires("openssl/1.1.1s")
            else:
                self.requires("openssl/1.1.1s")
        if self.options.get_safe("with_opentelemetry"):
            self.requires("opentelemetry-cpp/1.7.0")
        if self.options.with_s3:
            self.requires("aws-sdk-cpp/1.9.234")
        if self.options.with_brotli:
            self.requires("brotli/1.0.9")
        if self.options.with_bz2:
            self.requires("bzip2/1.0.8")
        if self.options.with_lz4:
            self.requires("lz4/1.9.4")
        if self.options.with_snappy:
            self.requires("snappy/1.1.9")
        if Version(self.version) >= "6.0.0" and \
            self.options.get_safe("simd_level") != None or \
            self.options.get_safe("runtime_simd_level") != None:
            self.requires("xsimd/9.0.1")
        if self.options.with_zlib:
            self.requires("zlib/1.2.13")
        if self.options.with_zstd:
            self.requires("zstd/1.5.2")
        if self._with_re2():
            self.requires("re2/20220601")
        if self._with_utf8proc():
            self.requires("utf8proc/2.8.0")
        if self.options.with_backtrace:
            self.requires("libbacktrace/cci.20210118")

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
                  destination=self.source_folder, strip_root=True)

    def generate(self):
        # BUILD_SHARED_LIBS and POSITION_INDEPENDENT_CODE are automatically parsed when self.options.shared or self.options.fPIC exist
        tc = CMakeToolchain(self)
        if cross_building(self):
            cmake_system_processor = {
                "armv8": "aarch64",
                "armv8.3": "aarch64",
            }.get(str(self.settings.arch), str(self.settings.arch))
            tc.variables["CMAKE_SYSTEM_PROCESSOR"] = cmake_system_processor
            if cmake_system_processor == "aarch64":
                tc.variables["ARROW_CPU_FLAG"] = "armv8"
        if is_msvc(self):
            tc.variables["ARROW_USE_STATIC_CRT"] = is_msvc_static_runtime(self)
        tc.variables["ARROW_DEPENDENCY_SOURCE"] = "SYSTEM"
        tc.variables["ARROW_PACKAGE_KIND"] = "conan"
        tc.variables["ARROW_GANDIVA"] = bool(self.options.gandiva)
        tc.variables["ARROW_PARQUET"] = self._parquet()
        tc.variables["ARROW_SUBSTRAIT"] = bool(self.options.get_safe("substrait", False))
        tc.variables["ARROW_PLASMA"] = bool(self.options.plasma)
        tc.variables["ARROW_DATASET"] = self._dataset_modules()
        tc.variables["ARROW_FILESYSTEM"] = bool(self.options.filesystem_layer)
        tc.variables["PARQUET_REQUIRE_ENCRYPTION"] = bool(self.options.encryption)
        tc.variables["ARROW_HDFS"] = bool(self.options.hdfs_bridgs)
        tc.variables["ARROW_VERBOSE_THIRDPARTY_BUILD"] = True
        tc.variables["ARROW_BUILD_SHARED"] = bool(self.options.shared)
        tc.variables["ARROW_BUILD_STATIC"] = not bool(self.options.shared)
        tc.variables["ARROW_NO_DEPRECATED_API"] = not bool(self.options.deprecated)
        tc.variables["ARROW_FLIGHT"] = self._with_flight_rpc()
        tc.variables["ARROW_FLIGHT_SQL"] = bool(self.options.get_safe("with_flight_sql", False))
        tc.variables["ARROW_COMPUTE"] = self._compute()
        tc.variables["ARROW_CSV"] = bool(self.options.with_csv)
        tc.variables["ARROW_CUDA"] = bool(self.options.with_cuda)
        tc.variables["ARROW_JEMALLOC"] = self._with_jemalloc()
        tc.variables["ARROW_MIMALLOC"] = bool(self.options.with_mimalloc)
        tc.variables["jemalloc_SOURCE"] = "SYSTEM"
        tc.variables["ARROW_JSON"] = bool(self.options.with_json)
        tc.variables["google_cloud_cpp_SOURCE"] = "SYSTEM"
        tc.variables["ARROW_GCS"] = bool(self.options.get_safe("with_gcs", False))
        tc.variables["BOOST_SOURCE"] = "SYSTEM"
        tc.variables["Protobuf_SOURCE"] = "SYSTEM"
        if self._with_protobuf():
            tc.variables["ARROW_PROTOBUF_USE_SHARED"] = bool(self.options["protobuf"].shared)
        tc.variables["gRPC_SOURCE"] = "SYSTEM"
        if self._with_grpc():
            tc.variables["ARROW_GRPC_USE_SHARED"] = bool(self.options["grpc"].shared)

        tc.variables["ARROW_USE_GLOG"] = self._with_glog()
        tc.variables["GLOG_SOURCE"] = "SYSTEM"
        tc.variables["ARROW_WITH_BACKTRACE"] = bool(self.options.with_backtrace)
        tc.variables["ARROW_WITH_BROTLI"] = bool(self.options.with_brotli)
        tc.variables["brotli_SOURCE"] = "SYSTEM"
        if self.options.with_brotli:
            tc.variables["ARROW_BROTLI_USE_SHARED"] = bool(self.options["brotli"].shared)
        tc.variables["gflags_SOURCE"] = "SYSTEM"
        if self._with_gflags():
            tc.variables["ARROW_GFLAGS_USE_SHARED"] = bool(self.options["gflags"].shared)
        tc.variables["ARROW_WITH_BZ2"] = bool(self.options.with_bz2)
        tc.variables["BZip2_SOURCE"] = "SYSTEM"
        if self.options.with_bz2:
            tc.variables["ARROW_BZ2_USE_SHARED"] = bool(self.options["bzip2"].shared)
        tc.variables["ARROW_WITH_LZ4"] = bool(self.options.with_lz4)
        tc.variables["lz4_SOURCE"] = "SYSTEM"
        if self.options.with_lz4:
            tc.variables["ARROW_LZ4_USE_SHARED"] = bool(self.options["lz4"].shared)
        tc.variables["ARROW_WITH_SNAPPY"] = bool(self.options.with_snappy)
        tc.variables["RapidJSON_SOURCE"] = "SYSTEM"
        tc.variables["Snappy_SOURCE"] = "SYSTEM"
        if self.options.with_snappy:
            tc.variables["ARROW_SNAPPY_USE_SHARED"] = bool(self.options["snappy"].shared)
        tc.variables["ARROW_WITH_ZLIB"] = bool(self.options.with_zlib)
        tc.variables["re2_SOURCE"] = "SYSTEM"
        tc.variables["ZLIB_SOURCE"] = "SYSTEM"
        tc.variables["xsimd_SOURCE"] = "SYSTEM"
        tc.variables["ARROW_WITH_ZSTD"] = bool(self.options.with_zstd)
        if Version(self.version) >= "2.0":
            tc.variables["zstd_SOURCE"] = "SYSTEM"
            tc.variables["ARROW_SIMD_LEVEL"] = str(self.options.simd_level).upper()
            tc.variables["ARROW_RUNTIME_SIMD_LEVEL"] = str(self.options.runtime_simd_level).upper()
        else:
            tc.variables["ZSTD_SOURCE"] = "SYSTEM"
        if self.options.with_zstd:
            tc.variables["ARROW_ZSTD_USE_SHARED"] = bool(self.options["zstd"].shared)
        tc.variables["ORC_SOURCE"] = "SYSTEM"
        tc.variables["ARROW_WITH_THRIFT"] = self._with_thrift()
        tc.variables["Thrift_SOURCE"] = "SYSTEM"
        if self._with_thrift():
            tc.variables["THRIFT_VERSION"] = bool(self.deps_cpp_info["thrift"].version) # a recent thrift does not require boost
            tc.variables["ARROW_THRIFT_USE_SHARED"] = bool(self.options["thrift"].shared)
        tc.variables["ARROW_USE_OPENSSL"] = self._with_openssl()
        if self._with_openssl():
            tc.variables["OPENSSL_ROOT_DIR"] = self.deps_cpp_info["openssl"].rootpath.replace("\\", "/")
            tc.variables["ARROW_OPENSSL_USE_SHARED"] = bool(self.options["openssl"].shared)
        if self._with_boost():
            tc.variables["ARROW_USE_BOOST"] = True
            tc.variables["ARROW_BOOST_USE_SHARED"] = bool(self.options["boost"].shared)
        tc.variables["ARROW_S3"] = bool(self.options.with_s3)
        tc.variables["AWSSDK_SOURCE"] = "SYSTEM"
        tc.variables["ARROW_BUILD_UTILITIES"] = bool(self.options.cli)
        tc.variables["ARROW_BUILD_INTEGRATION"] = False
        tc.variables["ARROW_INSTALL_NAME_RPATH"] = False
        tc.variables["ARROW_BUILD_EXAMPLES"] = False
        tc.variables["ARROW_BUILD_TESTS"] = False
        tc.variables["ARROW_ENABLE_TIMING_TESTS"] = False
        tc.variables["ARROW_BUILD_BENCHMARKS"] = False
        tc.variables["LLVM_SOURCE"] = "SYSTEM"
        tc.variables["ARROW_WITH_UTF8PROC"] = self._with_utf8proc()
        tc.variables["ARROW_BOOST_REQUIRED"] = self._with_boost()
        tc.variables["utf8proc_SOURCE"] = "SYSTEM"
        if self._with_utf8proc():
            tc.variables["ARROW_UTF8PROC_USE_SHARED"] = bool(self.options["utf8proc"].shared)
        tc.variables["BUILD_WARNING_LEVEL"] = "PRODUCTION"
        if is_msvc(self):
            tc.variables["ARROW_USE_STATIC_CRT"] = "MT" in str(self.settings.compiler.runtime)
        if self._with_llvm():
            tc.variables["LLVM_DIR"] = self.deps_cpp_info["llvm-core"].rootpath.replace("\\", "/")
        tc.generate()

        deps = CMakeDeps(self)
        deps.generate()

    def _patch_sources(self):
        apply_conandata_patches(self)
        if Version(self.version) >= "7.0.0" and Version(self.version) < "11.0.0":
            for filename in glob.glob(os.path.join(self.source_folder, "cpp", "cmake_modules", "Find*.cmake")):
                if os.path.basename(filename) not in [
                    "FindArrow.cmake",
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
                    "FindPlasma.cmake",
                ]:
                    os.remove(filename)

    def build(self):
        self._patch_sources()
        cmake =CMake(self)
        cmake.configure(build_script_folder=os.path.join(self.source_folder, "cpp"))
        cmake.build()

    def package(self):
        copy(self, pattern="LICENSE.txt", dst=os.path.join(self.package_folder, "licenses"), src=self.source_folder)
        copy(self, pattern="NOTICE.txt", dst=os.path.join(self.package_folder, "licenses"), src=self.source_folder)
        cmake =CMake(self)
        cmake.install()

        rmdir(self, os.path.join(self.package_folder, "lib", "cmake"))
        rmdir(self, os.path.join(self.package_folder, "lib", "pkgconfig"))
        rmdir(self, os.path.join(self.package_folder, "share"))

    def _lib_name(self, name):
        if is_msvc(self) and not self.options.shared:
            return "{}_static".format(name)
        else:
            return "{}".format(name)

    def package_id(self):
        self.info.options.with_gflags = self._with_gflags()
        self.info.options.with_protobuf = self._with_protobuf()
        self.info.options.with_re2 = self._with_re2()
        self.info.options.with_jemalloc = self._with_jemalloc()
        self.info.options.with_openssl = self._with_openssl()
        self.info.options.with_boost = self._with_boost()
        self.info.options.with_glog = self._with_glog()
        self.info.options.with_grpc = self._with_grpc()

    def package_info(self):
        self.cpp_info.filenames["cmake_find_package"] = "Arrow"
        self.cpp_info.filenames["cmake_find_package_multi"] = "Arrow"
        self.cpp_info.components["libarrow"].libs = [self._lib_name("arrow")]
        self.cpp_info.components["libarrow"].names["cmake_find_package"] = "arrow"
        self.cpp_info.components["libarrow"].names["cmake_find_package_multi"] = "arrow"
        self.cpp_info.components["libarrow"].names["pkg_config"] = "arrow"
        if not self.options.shared:
            self.cpp_info.components["libarrow"].defines = ["ARROW_STATIC"]
            if self.settings.os in ["Linux", "FreeBSD"]:
                self.cpp_info.components["libarrow"].system_libs = ["pthread", "m", "dl", "rt"]

        if self._parquet():
            self.cpp_info.components["libparquet"].libs = [self._lib_name("parquet")]
            self.cpp_info.components["libparquet"].names["cmake_find_package"] = "parquet"
            self.cpp_info.components["libparquet"].names["cmake_find_package_multi"] = "parquet"
            self.cpp_info.components["libparquet"].names["pkg_config"] = "parquet"
            self.cpp_info.components["libparquet"].requires = ["libarrow"]

        if self.options.get_safe("substrait", False):
            self.cpp_info.components["libarrow_substrait"].libs = [self._lib_name("arrow_substrait")]
            self.cpp_info.components["libarrow_substrait"].names["cmake_find_package"] = "arrow_substrait"
            self.cpp_info.components["libarrow_substrait"].names["cmake_find_package_multi"] = "arrow_substrait"
            self.cpp_info.components["libarrow_substrait"].names["pkg_config"] = "arrow_substrait"
            self.cpp_info.components["libarrow_substrait"].requires = ["libparquet", "dataset"]

        if self.options.plasma:
            self.cpp_info.components["libplasma"].libs = [self._lib_name("plasma")]
            self.cpp_info.components["libplasma"].names["cmake_find_package"] = "plasma"
            self.cpp_info.components["libplasma"].names["cmake_find_package_multi"] = "plasma"
            self.cpp_info.components["libplasma"].names["pkg_config"] = "plasma"
            self.cpp_info.components["libplasma"].requires = ["libarrow"]

        if self.options.gandiva:
            self.cpp_info.components["libgandiva"].libs = [self._lib_name("gandiva")]
            self.cpp_info.components["libgandiva"].names["cmake_find_package"] = "gandiva"
            self.cpp_info.components["libgandiva"].names["cmake_find_package_multi"] = "gandiva"
            self.cpp_info.components["libgandiva"].names["pkg_config"] = "gandiva"
            self.cpp_info.components["libgandiva"].requires = ["libarrow"]

        if self._with_flight_rpc():
            self.cpp_info.components["libarrow_flight"].libs = [self._lib_name("arrow_flight")]
            self.cpp_info.components["libarrow_flight"].names["cmake_find_package"] = "flight_rpc"
            self.cpp_info.components["libarrow_flight"].names["cmake_find_package_multi"] = "flight_rpc"
            self.cpp_info.components["libarrow_flight"].names["pkg_config"] = "flight_rpc"
            self.cpp_info.components["libarrow_flight"].requires = ["libarrow"]

        if self.options.get_safe("with_flight_sql"):
            self.cpp_info.components["libarrow_flight_sql"].libs = [self._lib_name("arrow_flight_sql")]
            self.cpp_info.components["libarrow_flight_sql"].names["cmake_find_package"] = "flight_sql"
            self.cpp_info.components["libarrow_flight_sql"].names["cmake_find_package_multi"] = "flight_sql"
            self.cpp_info.components["libarrow_flight_sql"].names["pkg_config"] = "flight_sql"
            self.cpp_info.components["libarrow_flight_sql"].requires = ["libarrow", "libarrow_flight"]

        if self._dataset_modules():
            self.cpp_info.components["dataset"].libs = ["arrow_dataset"]

        if (self.options.cli and (self.options.with_cuda or self._with_flight_rpc() or self._parquet())) or self.options.plasma:
            binpath = os.path.join(self.package_folder, "bin")
            self.output.info(f"Appending PATH env var: {binpath}")
            self.env_info.PATH.append(binpath)

        if self._with_boost():
            if self.options.gandiva:
                # FIXME: only filesystem component is used
                self.cpp_info.components["libgandiva"].requires.append("boost::boost")
            if self._parquet() and self.settings.compiler == "gcc" and self.settings.compiler.version < Version("4.9"):
                self.cpp_info.components["libparquet"].requires.append("boost::boost")
            if Version(self.version) >= "2.0":
                # FIXME: only headers components is used
                self.cpp_info.components["libarrow"].requires.append("boost::boost")
        if self._with_openssl():
            self.cpp_info.components["libarrow"].requires.append("openssl::openssl")
        if self._with_gflags():
            self.cpp_info.components["libarrow"].requires.append("gflags::gflags")
        if self._with_glog():
            self.cpp_info.components["libarrow"].requires.append("glog::glog")
        if self._with_jemalloc():
            self.cpp_info.components["libarrow"].requires.append("jemalloc::jemalloc")
        if self.options.with_mimalloc:
            self.cpp_info.components["libarrow"].requires.append("mimalloc::mimalloc")
        if self._with_re2():
            self.cpp_info.components["libgandiva"].requires.append("re2::re2")
        if self._with_llvm():
            self.cpp_info.components["libgandiva"].requires.append("llvm-core::llvm-core")
        if self._with_protobuf():
            self.cpp_info.components["libarrow"].requires.append("protobuf::protobuf")
        if self._with_utf8proc():
            self.cpp_info.components["libarrow"].requires.append("utf8proc::utf8proc")
        if self._with_thrift():
            self.cpp_info.components["libarrow"].requires.append("thrift::thrift")
        if self.options.with_backtrace:
            self.cpp_info.components["libarrow"].requires.append("libbacktrace::libbacktrace")
        if self.options.with_cuda:
            self.cpp_info.components["libarrow"].requires.append("cuda::cuda")
        if self._with_rapidjson():
            self.cpp_info.components["libarrow"].requires.append("rapidjson::rapidjson")
        if self.options.with_s3:
            self.cpp_info.components["libarrow"].requires.append("aws-sdk-cpp::s3")
        if self.options.get_safe("with_gcs"):
            self.cpp_info.components["libarrow"].requires.append("google-cloud-cpp::storage")
        if self.options.with_orc:
            self.cpp_info.components["libarrow"].requires.append("orc::orc")
        if self.options.get_safe("with_opentelemetry"):
            self.cpp_info.components["libarrow"].requires.append("opentelemetry-cpp::opentelemetry-cpp")
        if self.options.with_brotli:
            self.cpp_info.components["libarrow"].requires.append("brotli::brotli")
        if self.options.with_bz2:
            self.cpp_info.components["libarrow"].requires.append("bzip2::bzip2")
        if self.options.with_lz4:
            self.cpp_info.components["libarrow"].requires.append("lz4::lz4")
        if self.options.with_snappy:
            self.cpp_info.components["libarrow"].requires.append("snappy::snappy")
        if self.options.get_safe("simd_level") != None or self.options.get_safe("runtime_simd_level") != None:
            self.cpp_info.components["libarrow"].requires.append("xsimd::xsimd")
        if self.options.with_zlib:
            self.cpp_info.components["libarrow"].requires.append("zlib::zlib")
        if self.options.with_zstd:
            self.cpp_info.components["libarrow"].requires.append("zstd::zstd")
        if self._with_boost():
            self.cpp_info.components["libarrow"].requires.append("boost::boost")
        if self._with_grpc():
            self.cpp_info.components["libarrow"].requires.append("grpc::grpc")
        if self._with_flight_rpc():
            self.cpp_info.components["libarrow_flight"].requires.append("protobuf::protobuf")
