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

# Cloned from https://github.com/autobrew/homebrew-core/blob/high-sierra/Formula/apache-arrow.rb
# Upstream any relevant changes (dependencies, build args ...)

class ApacheArrowStatic < Formula
  desc "Columnar in-memory analytics layer designed to accelerate big data"
  homepage "https://arrow.apache.org/"
  url "https://www.apache.org/dyn/closer.lua?path=arrow/arrow-8.0.0.9000/apache-arrow-8.0.0.9000.tar.gz"
  # Uncomment and update to test on a release candidate
  # mirror "https://dist.apache.org/repos/dist/dev/arrow/apache-arrow-8.0.0-rc1/apache-arrow-8.0.0.tar.gz"
  sha256 "9948ddb6d4798b51552d0dca3252dd6e3a7d0f9702714fc6f5a1b59397ce1d28"
  head "https://github.com/apache/arrow.git"

  bottle do
    sha256 cellar: :any, arm64_big_sur: "ef89d21a110b89840cc6148add685d407e75bd633bc8f79625eb33d00e3694b4"
    sha256 cellar: :any, big_sur:       "6fcb9f55d44eb61d295a8020e039a0622bdc044b220cfffef855f3e3ab8057a1"
    sha256 cellar: :any, catalina:      "bf71b17766688077fb9b4e679f309742c16524015287dd3633758c679752c977"
  end

  depends_on "boost" => :build
  depends_on "cmake" => :build
  depends_on "aws-sdk-cpp-static"
  depends_on "brotli"
  depends_on "lz4"
  depends_on "snappy"
  depends_on "thrift"
  depends_on "zstd"

  conflicts_with "apache-arrow", because: "both install Arrow"

  def install
    ENV.cxx11
    # https://github.com/Homebrew/homebrew-core/issues/94724
    # https://issues.apache.org/jira/browse/ARROW-15664
    ENV["HOMEBREW_OPTIMIZATION_LEVEL"] = "O2"

    args = %W[
      -DARROW_BUILD_SHARED=OFF
      -DARROW_BUILD_UTILITIES=ON
      -DARROW_ACERO=ON
      -DARROW_COMPUTE=ON
      -DARROW_CSV=ON
      -DARROW_DATASET=ON
      -DARROW_FILESYSTEM=ON
      -DARROW_GCS=ON
      -DARROW_JEMALLOC=ON
      -DARROW_JSON=ON
      -DARROW_MIMALLOC=ON
      -DARROW_PARQUET=ON
      -DARROW_S3=ON
      -DARROW_VERBOSE_THIRDPARTY_BUILD=ON
      -DARROW_WITH_BROTLI=ON
      -DARROW_WITH_BZ2=ON
      -DARROW_WITH_LZ4=ON
      -DARROW_WITH_SNAPPY=ON
      -DARROW_WITH_ZLIB=ON
      -DARROW_WITH_ZSTD=ON
      -DLZ4_HOME=#{Formula["lz4"].prefix}
      -DPARQUET_BUILD_EXECUTABLES=ON
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
    system ENV.cxx, "test.cpp", "-std=c++17", "-I#{include}", "-L#{lib}", \
      "-larrow", "-larrow_bundled_dependencies", "-o", "test"
    system "./test"
  end
end
