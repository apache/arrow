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

# https://github.com/autobrew/homebrew-cran/blob/master/Formula/apache-arrow.rb
class ApacheArrowStatic < Formula
  desc "Columnar in-memory analytics layer designed to accelerate big data"
  homepage "https://arrow.apache.org/"
  url "https://www.apache.org/dyn/closer.lua?path=arrow/arrow-8.0.0.9000/apache-arrow-8.0.0.9000.tar.gz"
  # Uncomment and update to test on a release candidate
  # mirror "https://dist.apache.org/repos/dist/dev/arrow/apache-arrow-8.0.0-rc1/apache-arrow-8.0.0.tar.gz"
  sha256 "9948ddb6d4798b51552d0dca3252dd6e3a7d0f9702714fc6f5a1b59397ce1d28"
  head "https://github.com/apache/arrow.git"

  # Linux bottle removed for GCC 12 migration
  bottle do
    sha256 cellar: :any,                 arm64_monterey: "236f3e3d792c5450549703d4baaffb4c272c10495ab5e28e00745e33dd5c8b9f"
    sha256 cellar: :any,                 arm64_big_sur:  "4f410eb8437deb23972cc7c1afee02f589debed45c0e2cd70b5f30735a45a94a"
    sha256 cellar: :any,                 monterey:       "b523d867a39dcd7bb1e6184bf542a7dbe2dcecdf4ce987413e0a4beaa86dcd67"
    sha256 cellar: :any,                 big_sur:        "6c94f65c8985387ff46b26b2d0e6a2a3cf6ce2c34d264d2353580ba73c0fdc7e"
    sha256 cellar: :any,                 catalina:       "1bb210d9cb9bbd851c1d0d51ad88ffc2516f35d87f4b3771b4d4313e65576a68"
  end

  depends_on "aws-sdk-cpp"
  depends_on "boost" => :build
  depends_on "brotli"
  depends_on "cmake" => :build
  depends_on "glog"
  depends_on "grpc"
  depends_on "llvm" => :build
  depends_on "lz4"
  depends_on "numpy"
  depends_on "openssl@1.1"
  depends_on "protobuf"
  depends_on "python@3.10"
  depends_on "rapidjson"
  depends_on "re2"
  depends_on "snappy"
  depends_on "thrift"
  depends_on "utf8proc"
  depends_on "zstd"
  
  fails_with gcc: "5"

  def install
    # https://github.com/Homebrew/homebrew-core/issues/76537
    ENV.runtime_cpu_detection if Hardware::CPU.intel?

    # https://github.com/Homebrew/homebrew-core/issues/94724
    # https://issues.apache.org/jira/browse/ARROW-15664
    ENV["HOMEBREW_OPTIMIZATION_LEVEL"] = "O2"

    # link against system libc++ instead of llvm provided libc++
    ENV.remove "HOMEBREW_LIBRARY_PATHS", Formula["llvm"].opt_lib
    args = %W[
      -DARROW_COMPUTE=ON
      -DARROW_CSV=ON
      -DARROW_DATASET=ON
      -DARROW_FILESYSTEM=ON
      -DARROW_FLIGHT=ON
      -DARROW_GANDIVA=ON
      -DARROW_GCS=ON
      -DARROW_INSTALL_NAME_RPATH=OFF
      -DARROW_JEMALLOC=ON
      -DARROW_JSON=ON
      -DARROW_MIMALLOC=ON
      -DARROW_ORC=ON
      -DARROW_PARQUET=ON
      -DARROW_PLASMA=ON
      -DARROW_PROTOBUF_USE_SHARED=ON
      -DARROW_PYTHON=ON
      -DARROW_S3=ON
      -DARROW_USE_GLOG=OFF
      -DARROW_WITH_BROTLI=ON
      -DARROW_WITH_BZ2=ON
      -DARROW_WITH_LZ4=ON
      -DARROW_WITH_SNAPPY=ON
      -DARROW_WITH_UTF8PROC=ON
      -DARROW_WITH_ZLIB=ON
      -DARROW_WITH_ZSTD=ON
      -DCMAKE_CXX_STANDARD=11 
      -DCMAKE_FIND_PACKAGE_PREFER_CONFIG=TRUE
      -DLZ4_HOME=#{Formula["lz4"].prefix}
      -DPython3_EXECUTABLE=#{Formula["python@3.9"].bin/"python3"}
      -DTHRIFT_HOME=#{Formula["thrift"].prefix}
    ]

    mkdir "build"
    cd "build" do
      system "cmake", "../cpp", *std_cmake_args, *args
      system "make"
      system "make", "install"
    end
  end

  test do
    (testpath/"test.cpp").write <<~EOS
      #include "arrow/api.h"
      int main(void) {
        arrow::int64();
        return 0;
      }
    EOS
    system ENV.cxx, "test.cpp", "-std=c++11", "-I#{include}", "-L#{lib}", "-larrow", "-o", "test"
    system "./test"
  end
end