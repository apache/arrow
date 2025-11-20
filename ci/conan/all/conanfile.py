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

import os

from conan import ConanFile
from conan.errors import ConanInvalidConfiguration, ConanException
from conan.tools.build import check_min_cppstd, cross_building
from conan.tools.cmake import CMake, CMakeDeps, CMakeToolchain, cmake_layout
from conan.tools.files import apply_conandata_patches, copy, export_conandata_patches, get, rmdir
from conan.tools.microsoft import is_msvc, is_msvc_static_runtime
from conan.tools.scm import Version

required_conan_version = ">=2.1.0"


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
        "parquet": True,
        "skyhook": False,
        "substrait": False,
        "acero": False,
        "cli": False,
        "compute": False,
        "dataset_modules": False,
        "deprecated": True,
        "encryption": False,
        "filesystem_layer": True,
        "hdfs_bridgs": False,
        "plasma": "deprecated",
        "simd_level": "default",
        "runtime_simd_level": "max",
        "with_backtrace": False,
        "with_boost": True,
        "with_brotli": False,
        "with_bz2": False,
        "with_csv": False,
        "with_cuda": False,
        "with_flight_rpc": False,
        "with_flight_sql": False,
        "with_gcs": False,
        "with_gflags": False,
        "with_jemalloc": False,
        "with_mimalloc": False,
        "with_glog": False,
        "with_grpc": False,
        "with_json": False,
        "with_thrift": True,
        "with_llvm": False,
        "with_openssl": False,
        "with_opentelemetry": False,
        "with_orc": False,
        "with_protobuf": False,
        "with_re2": False,
        "with_s3": False,
        "with_utf8proc": False,
        "with_lz4": False,
        "with_snappy": False,
        "with_zlib": True,
        "with_zstd": False,
    }
    short_paths = True

    @property
    def _min_cppstd(self):
        # arrow >= 10.0.0 requires C++17.
        # https://github.com/apache/arrow/pull/13991
        return "17"

    def export_sources(self):
        export_conandata_patches(self)
        copy(self, "conan_cmake_project_include.cmake", self.recipe_folder, os.path.join(self.export_sources_folder, "src"))

    def config_options(self):
        if self.settings.os == "Windows":
            del self.options.fPIC
        if is_msvc(self):
            self.options.with_boost = True
        if Version(self.version) >= "19.0.0":
            self.options.with_mimalloc = True

    def configure(self):
        if self.options.shared:
            self.options.rm_safe("fPIC")

    def layout(self):
        cmake_layout(self, src_folder="src")

    def _requires_rapidjson(self):
        return self.options.with_json or self.options.encryption

    def requirements(self):
        if self.options.with_thrift:
            self.requires("thrift/0.20.0")
        if self.options.with_protobuf:
            self.requires("protobuf/3.21.12")
        if self.options.with_jemalloc:
            self.requires("jemalloc/5.3.0")
        if self.options.with_mimalloc:
            self.requires("mimalloc/1.7.6")
        if self.options.with_boost:
            self.requires("boost/1.85.0")
        if self.options.with_gflags:
            self.requires("gflags/2.2.2")
        if self.options.with_glog:
            self.requires("glog/0.6.0")
        if self.options.get_safe("with_gcs"):
            self.requires("google-cloud-cpp/1.40.1")
        if self.options.with_grpc:
            self.requires("grpc/1.50.0")
        if self._requires_rapidjson():
            self.requires("rapidjson/1.1.0")
        if self.options.with_llvm:
            self.requires("llvm-core/13.0.0")
        if self.options.with_openssl:
            # aws-sdk-cpp requires openssl/1.1.1. it uses deprecated functions in openssl/3.0.0
            if self.options.with_s3:
                self.requires("openssl/1.1.1w")
            else:
                self.requires("openssl/[>=1.1 <4]")
        if self.options.get_safe("with_opentelemetry"):
            self.requires("opentelemetry-cpp/1.7.0")
        if self.options.with_s3:
            self.requires("aws-sdk-cpp/1.9.234")
        if self.options.with_brotli:
            self.requires("brotli/1.1.0")
        if self.options.with_bz2:
            self.requires("bzip2/1.0.8")
        if self.options.with_lz4:
            self.requires("lz4/1.9.4")
        if self.options.with_snappy:
            self.requires("snappy/1.1.9")
        if self.options.get_safe("simd_level") != None or \
                self.options.get_safe("runtime_simd_level") != None:
                self.requires("xsimd/13.0.0")
        if self.options.with_zlib:
            self.requires("zlib/[>=1.2.11 <2]")
        if self.options.with_zstd:
            self.requires("zstd/[>=1.5 <1.6]")
        if self.options.with_re2:
            self.requires("re2/20230301")
        if self.options.with_utf8proc:
            self.requires("utf8proc/2.8.0")
        if self.options.with_backtrace:
            self.requires("libbacktrace/cci.20210118")
        if self.options.with_orc:
            self.requires("orc/2.0.0")

    def validate(self):
        # Do not allow options with 'auto' value
        # TODO: Remove "auto" from the possible values for these options
        auto_options = [option for option, value in self.options.items() if value == "auto"]
        if auto_options:
            raise ConanException("Options with value 'auto' are deprecated. Please set them true/false or use its default value."
                                 f" Please change the following options: {auto_options}")

        # From https://github.com/conan-io/conan-center-index/pull/23163#issuecomment-2039808851
        if self.options.gandiva:
            if not self.options.with_re2:
                raise ConanException("'with_re2' option should be True when 'gandiva=True'")
            if not self.options.with_boost:
                raise ConanException("'with_boost' option should be True when 'gandiva=True'")
            if not self.options.with_utf8proc:
                raise ConanException("'with_utf8proc' option should be True when 'gandiva=True'")
        if self.options.with_orc:
            if not self.options.with_lz4:
                raise ConanException("'with_lz4' option should be True when 'orc=True'")
            if not self.options.with_snappy:
                raise ConanException("'with_snappy' option should be True when 'orc=True'")
            if not self.options.with_zlib:
                raise ConanException("'with_zlib' option should be True when 'orc=True'")
            if not self.options.with_zstd:
                raise ConanException("'with_zstd' option should be True when 'orc=True'")
        if self.options.with_thrift and not self.options.with_boost:
            raise ConanException("'with_boost' option should be True when 'thrift=True'")
        if self.options.parquet:
            if not self.options.with_thrift:
                raise ConanException("'with_thrift' option should be True when 'parquet=True'")
        if self.options.with_flight_rpc and not self.options.with_protobuf:
            raise ConanException("'with_protobuf' option should be True when 'with_flight_rpc=True'")

        check_min_cppstd(self, self._min_cppstd)

        if self.options.get_safe("skyhook", False):
            raise ConanInvalidConfiguration("CCI has no librados recipe (yet)")
        if self.options.with_cuda:
            raise ConanInvalidConfiguration("CCI has no cuda recipe (yet)")
        if self.options.with_s3 and not self.dependencies["aws-sdk-cpp"].options.config:
            raise ConanInvalidConfiguration("arrow:with_s3 requires aws-sdk-cpp:config is True.")

        if self.options.shared and self.options.with_jemalloc:
            if self.dependencies["jemalloc"].options.enable_cxx:
                raise ConanInvalidConfiguration("jemmalloc.enable_cxx of a static jemalloc must be disabled")

        if self.options.with_thrift and not self.options.with_zlib:
            raise ConanInvalidConfiguration("arrow:with_thrift requires arrow:with_zlib")

        if self.options.parquet and not self.options.with_thrift:
            raise ConanInvalidConfiguration("arrow:parquet requires arrow:with_thrift")

    def build_requirements(self):
        if Version(self.version) >= "20.0.0":
            self.tool_requires("cmake/[>=3.25 <4]")
        else:
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
        self._patch_sources()

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
        tc.variables["ARROW_GANDIVA"] = bool(self.options.gandiva)
        tc.variables["ARROW_PARQUET"] = self.options.parquet
        tc.variables["ARROW_SUBSTRAIT"] = bool(self.options.get_safe("substrait", False))
        tc.variables["ARROW_ACERO"] = bool(self.options.acero)
        tc.variables["ARROW_DATASET"] = self.options.dataset_modules
        tc.variables["ARROW_FILESYSTEM"] = bool(self.options.filesystem_layer)
        tc.variables["PARQUET_REQUIRE_ENCRYPTION"] = bool(self.options.encryption)
        tc.variables["ARROW_HDFS"] = bool(self.options.hdfs_bridgs)
        tc.variables["ARROW_VERBOSE_THIRDPARTY_BUILD"] = True
        tc.variables["ARROW_BUILD_SHARED"] = bool(self.options.shared)
        tc.variables["ARROW_BUILD_STATIC"] = not bool(self.options.shared)
        tc.variables["ARROW_NO_DEPRECATED_API"] = not bool(self.options.deprecated)
        tc.variables["ARROW_FLIGHT"] = self.options.with_flight_rpc
        tc.variables["ARROW_FLIGHT_SQL"] = bool(self.options.get_safe("with_flight_sql", False))
        tc.variables["ARROW_COMPUTE"] = bool(self.options.compute)
        tc.variables["ARROW_CSV"] = bool(self.options.with_csv)
        tc.variables["ARROW_CUDA"] = bool(self.options.with_cuda)
        tc.variables["ARROW_JEMALLOC"] = self.options.with_jemalloc
        tc.variables["jemalloc_SOURCE"] = "SYSTEM"
        tc.variables["ARROW_MIMALLOC"] = bool(self.options.with_mimalloc)
        tc.variables["ARROW_JSON"] = bool(self.options.with_json)
        tc.variables["google_cloud_cpp_SOURCE"] = "SYSTEM"
        tc.variables["ARROW_GCS"] = bool(self.options.get_safe("with_gcs", False))
        tc.variables["BOOST_SOURCE"] = "SYSTEM"
        tc.variables["Protobuf_SOURCE"] = "SYSTEM"
        if self.options.with_protobuf:
            tc.variables["ARROW_PROTOBUF_USE_SHARED"] = bool(self.dependencies["protobuf"].options.shared)
        tc.variables["gRPC_SOURCE"] = "SYSTEM"
        if self.options.with_grpc:
            tc.variables["ARROW_GRPC_USE_SHARED"] = bool(self.dependencies["grpc"].options.shared)

        tc.variables["ARROW_USE_GLOG"] = self.options.with_glog
        tc.variables["GLOG_SOURCE"] = "SYSTEM"
        tc.variables["ARROW_WITH_BACKTRACE"] = bool(self.options.with_backtrace)
        tc.variables["ARROW_WITH_BROTLI"] = bool(self.options.with_brotli)
        tc.variables["ARROW_WITH_RE2"] = bool(self.options.with_re2)
        tc.variables["brotli_SOURCE"] = "SYSTEM"
        if self.options.with_brotli:
            tc.variables["ARROW_BROTLI_USE_SHARED"] = bool(self.dependencies["brotli"].options.shared)
        tc.variables["gflags_SOURCE"] = "SYSTEM"
        if self.options.with_gflags:
            tc.variables["ARROW_GFLAGS_USE_SHARED"] = bool(self.dependencies["gflags"].options.shared)
        tc.variables["ARROW_WITH_BZ2"] = bool(self.options.with_bz2)
        tc.variables["BZip2_SOURCE"] = "SYSTEM"
        if self.options.with_bz2:
            tc.variables["ARROW_BZ2_USE_SHARED"] = bool(self.dependencies["bzip2"].options.shared)
        tc.variables["ARROW_WITH_LZ4"] = bool(self.options.with_lz4)
        tc.variables["lz4_SOURCE"] = "SYSTEM"
        if self.options.with_lz4:
            tc.variables["ARROW_LZ4_USE_SHARED"] = bool(self.dependencies["lz4"].options.shared)
        tc.variables["ARROW_WITH_SNAPPY"] = bool(self.options.with_snappy)
        tc.variables["RapidJSON_SOURCE"] = "SYSTEM"
        tc.variables["Snappy_SOURCE"] = "SYSTEM"
        if self.options.with_snappy:
            tc.variables["ARROW_SNAPPY_USE_SHARED"] = bool(self.dependencies["snappy"].options.shared)
        tc.variables["ARROW_WITH_ZLIB"] = bool(self.options.with_zlib)
        tc.variables["re2_SOURCE"] = "SYSTEM"
        tc.variables["ZLIB_SOURCE"] = "SYSTEM"
        tc.variables["xsimd_SOURCE"] = "SYSTEM"
        tc.variables["ARROW_WITH_ZSTD"] = bool(self.options.with_zstd)
        tc.variables["zstd_SOURCE"] = "SYSTEM"
        tc.variables["ARROW_SIMD_LEVEL"] = str(self.options.simd_level).upper()
        tc.variables["ARROW_RUNTIME_SIMD_LEVEL"] = str(self.options.runtime_simd_level).upper()
        if self.options.with_zstd:
            tc.variables["ARROW_ZSTD_USE_SHARED"] = bool(self.dependencies["zstd"].options.shared)
        tc.variables["ORC_SOURCE"] = "SYSTEM"
        tc.variables["ARROW_ORC"] = bool(self.options.with_orc)
        tc.variables["ARROW_WITH_THRIFT"] = bool(self.options.with_thrift)
        tc.variables["ARROW_THRIFT"] = bool(self.options.with_thrift)
        tc.variables["Thrift_SOURCE"] = "SYSTEM"
        if self.options.with_thrift:
            tc.variables["ARROW_THRIFT"] = True
            tc.variables["THRIFT_VERSION"] = bool(self.dependencies["thrift"].ref.version) # a recent thrift does not require boost
            tc.variables["ARROW_THRIFT_USE_SHARED"] = bool(self.dependencies["thrift"].options.shared)
        tc.variables["ARROW_USE_OPENSSL"] = self.options.with_openssl
        if self.options.with_openssl:
            tc.variables["OPENSSL_ROOT_DIR"] = self.dependencies["openssl"].package_folder.replace("\\", "/")
            tc.variables["ARROW_OPENSSL_USE_SHARED"] = bool(self.dependencies["openssl"].options.shared)
        if self.options.with_boost:
            tc.variables["ARROW_USE_BOOST"] = True
            tc.variables["ARROW_BOOST_USE_SHARED"] = bool(self.dependencies["boost"].options.shared)
        tc.variables["ARROW_S3"] = bool(self.options.with_s3)
        tc.variables["AWSSDK_SOURCE"] = "SYSTEM"
        tc.variables["ARROW_BUILD_UTILITIES"] = bool(self.options.cli)
        tc.variables["ARROW_BUILD_INTEGRATION"] = False
        tc.variables["ARROW_INSTALL_NAME_RPATH"] = True
        tc.variables["ARROW_BUILD_EXAMPLES"] = False
        tc.variables["ARROW_BUILD_TESTS"] = False
        tc.variables["ARROW_ENABLE_TIMING_TESTS"] = False
        tc.variables["ARROW_BUILD_BENCHMARKS"] = False
        tc.variables["LLVM_SOURCE"] = "SYSTEM"
        tc.variables["ARROW_WITH_UTF8PROC"] = self.options.with_utf8proc
        tc.variables["ARROW_BOOST_REQUIRED"] = self.options.with_boost
        tc.variables["utf8proc_SOURCE"] = "SYSTEM"
        if self.options.with_utf8proc:
            tc.variables["ARROW_UTF8PROC_USE_SHARED"] = bool(self.dependencies["utf8proc"].options.shared)
        tc.variables["BUILD_WARNING_LEVEL"] = "PRODUCTION"
        if is_msvc(self):
            tc.variables["ARROW_USE_STATIC_CRT"] = is_msvc_static_runtime(self)
        if self.options.with_llvm:
            tc.variables["LLVM_DIR"] = self.dependencies["llvm-core"].package_folder.replace("\\", "/")

        tc.cache_variables["CMAKE_PROJECT_arrow_INCLUDE"] = os.path.join(self.source_folder, "conan_cmake_project_include.cmake")
        tc.generate()

        deps = CMakeDeps(self)
        deps.set_property("mimalloc", "cmake_target_name", "mimalloc::mimalloc")
        deps.generate()

    def _patch_sources(self):
        apply_conandata_patches(self)

    def build(self):
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

        suffix = "_static" if is_msvc(self) and not self.options.shared else ""
        cmake_suffix = "shared" if self.options.shared else "static"

        self.cpp_info.components["libarrow"].set_property("pkg_config_name", "arrow")
        self.cpp_info.components["libarrow"].set_property("cmake_target_name", f"Arrow::arrow_{cmake_suffix}")
        self.cpp_info.components["libarrow"].libs = [f"arrow{suffix}"]
        if not self.options.shared:
            self.cpp_info.components["libarrow"].defines = ["ARROW_STATIC"]
            if self.settings.os in ["Linux", "FreeBSD"]:
                self.cpp_info.components["libarrow"].system_libs = ["pthread", "m", "dl", "rt"]

        if self.options.parquet:
            self.cpp_info.components["libparquet"].set_property("pkg_config_name", "parquet")
            self.cpp_info.components["libparquet"].set_property("cmake_target_name", f"Parquet::parquet_{cmake_suffix}")
            self.cpp_info.components["libparquet"].libs = [f"parquet{suffix}"]
            self.cpp_info.components["libparquet"].requires = ["libarrow"]
            if not self.options.shared:
                self.cpp_info.components["libparquet"].defines = ["PARQUET_STATIC"]

        if self.options.get_safe("substrait"):
            self.cpp_info.components["libarrow_substrait"].set_property("pkg_config_name", "arrow_substrait")
            self.cpp_info.components["libarrow_substrait"].set_property("cmake_target_name", f"Arrow::arrow_substrait_{cmake_suffix}")
            self.cpp_info.components["libarrow_substrait"].libs = [f"arrow_substrait{suffix}"]
            self.cpp_info.components["libarrow_substrait"].requires = ["libparquet", "dataset"]

        # Plasma was deprecated in Arrow 12.0.0
        del self.options.plasma

        if self.options.acero:
            self.cpp_info.components["libacero"].set_property("pkg_config_name", "acero")
            self.cpp_info.components["libacero"].set_property("cmake_target_name", f"Acero::arrow_acero_{cmake_suffix}")
            self.cpp_info.components["libacero"].libs = [f"arrow_acero{suffix}"]
            self.cpp_info.components["libacero"].names["cmake_find_package"] = "acero"
            self.cpp_info.components["libacero"].names["cmake_find_package_multi"] = "acero"
            self.cpp_info.components["libacero"].names["pkg_config"] = "acero"
            self.cpp_info.components["libacero"].requires = ["libarrow"]

        if self.options.gandiva:
            self.cpp_info.components["libgandiva"].set_property("pkg_config_name", "gandiva")
            self.cpp_info.components["libgandiva"].set_property("cmake_target_name", f"Gandiva::gandiva_{cmake_suffix}")
            self.cpp_info.components["libgandiva"].libs = [f"gandiva{suffix}"]
            self.cpp_info.components["libgandiva"].requires = ["libarrow"]
            if not self.options.shared:
                self.cpp_info.components["libgandiva"].defines = ["GANDIVA_STATIC"]

        if self.options.with_flight_rpc:
            self.cpp_info.components["libarrow_flight"].set_property("pkg_config_name", "flight_rpc")
            self.cpp_info.components["libarrow_flight"].set_property("cmake_target_name", f"ArrowFlight::arrow_flight_{cmake_suffix}")
            self.cpp_info.components["libarrow_flight"].libs = [f"arrow_flight{suffix}"]
            self.cpp_info.components["libarrow_flight"].requires = ["libarrow"]
            # https://github.com/apache/arrow/pull/43137#pullrequestreview-2267476893
            if Version(self.version) >= "18.0.0" and self.options.with_openssl:
                self.cpp_info.components["libarrow_flight"].requires.append("openssl::openssl")

        if self.options.get_safe("with_flight_sql"):
            self.cpp_info.components["libarrow_flight_sql"].set_property("pkg_config_name", "flight_sql")
            self.cpp_info.components["libarrow_flight_sql"].set_property("cmake_target_name", f"ArrowFlightSql::arrow_flight_sql_{cmake_suffix}")
            self.cpp_info.components["libarrow_flight_sql"].libs = [f"arrow_flight_sql{suffix}"]
            self.cpp_info.components["libarrow_flight_sql"].requires = ["libarrow", "libarrow_flight"]

        if self.options.dataset_modules:
            self.cpp_info.components["dataset"].libs = ["arrow_dataset"]
            if self.options.parquet:
                self.cpp_info.components["dataset"].requires = ["libparquet"]
            if self.options.acero and Version(self.version) >= "19.0.0":
                self.cpp_info.components["dataset"].requires = ["libacero"]

        if self.options.cli and (self.options.with_cuda or self.options.with_flight_rpc or self.options.parquet):
            binpath = os.path.join(self.package_folder, "bin")
            self.output.info(f"Appending PATH env var: {binpath}")
            self.env_info.PATH.append(binpath)

        if self.options.with_boost:
            if self.options.gandiva:
                # FIXME: only filesystem component is used
                self.cpp_info.components["libgandiva"].requires.append("boost::boost")
            if self.options.parquet and self.settings.compiler == "gcc" and self.settings.compiler.version < Version("4.9"):
                self.cpp_info.components["libparquet"].requires.append("boost::boost")
            # FIXME: only headers components is used
            self.cpp_info.components["libarrow"].requires.append("boost::boost")
        if self.options.with_openssl:
            self.cpp_info.components["libarrow"].requires.append("openssl::openssl")
        if self.options.with_gflags:
            self.cpp_info.components["libarrow"].requires.append("gflags::gflags")
        if self.options.with_glog:
            self.cpp_info.components["libarrow"].requires.append("glog::glog")
        if self.options.with_jemalloc:
            self.cpp_info.components["libarrow"].requires.append("jemalloc::jemalloc")
        if self.options.with_mimalloc:
            self.cpp_info.components["libarrow"].requires.append("mimalloc::mimalloc")
        if self.options.with_re2:
            if self.options.gandiva:
                self.cpp_info.components["libgandiva"].requires.append("re2::re2")
            if self.options.parquet:
                self.cpp_info.components["libparquet"].requires.append("re2::re2")
            self.cpp_info.components["libarrow"].requires.append("re2::re2")
        if self.options.with_llvm:
            self.cpp_info.components["libgandiva"].requires.append("llvm-core::llvm-core")
        if self.options.with_protobuf:
            self.cpp_info.components["libarrow"].requires.append("protobuf::protobuf")
        if self.options.with_utf8proc:
            self.cpp_info.components["libarrow"].requires.append("utf8proc::utf8proc")
        if self.options.with_thrift:
            self.cpp_info.components["libarrow"].requires.append("thrift::thrift")
        if self.options.with_backtrace:
            self.cpp_info.components["libarrow"].requires.append("libbacktrace::libbacktrace")
        if self.options.with_cuda:
            self.cpp_info.components["libarrow"].requires.append("cuda::cuda")
        if self._requires_rapidjson():
            self.cpp_info.components["libarrow"].requires.append("rapidjson::rapidjson")
        if self.options.with_s3:
            # https://github.com/apache/arrow/blob/6b268f62a8a172249ef35f093009c740c32e1f36/cpp/src/arrow/CMakeLists.txt#L98
            self.cpp_info.components["libarrow"].requires.extend([f"aws-sdk-cpp::{x}" for x in ["cognito-identity", "core", "identity-management", "s3", "sts"]])
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
        if self.options.with_grpc:
            self.cpp_info.components["libarrow"].requires.append("grpc::grpc")
        if self.options.with_flight_rpc:
            self.cpp_info.components["libarrow_flight"].requires.append("protobuf::protobuf")
