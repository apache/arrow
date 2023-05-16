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

# https://github.com/autobrew/homebrew-core/blob/-/Formula/apache-arrow.rb
class ApacheArrow < Formula
  desc "Columnar in-memory analytics layer designed to accelerate big data"
  homepage "https://arrow.apache.org/"
  url "https://www.apache.org/dyn/closer.lua?path=arrow/arrow-12.0.0.9000/apache-arrow-12.0.0.9000.tar.gz"
  sha256 "9948ddb6d4798b51552d0dca3252dd6e3a7d0f9702714fc6f5a1b59397ce1d28"
  head "https://github.com/apache/arrow.git"

  bottle do
    cellar :any
    sha256 "9cd44700798638b5e3ee8774b3929f3fad815290d05572d1f39f01d6423eaad0" => :high_sierra
    root_url "https://autobrew.github.io/bottles"
  end

  # NOTE: if you add something here, be sure to add to PKG_LIBS in r/tools/autobrew
  depends_on "boost" => :build
  depends_on "cmake" => :build
  depends_on "aws-sdk-cpp"
  depends_on "brotli"
  depends_on "lz4"
  depends_on "snappy"
  depends_on "thrift"
  depends_on "zstd"

  def install
    ENV.cxx11
    args = %W[
      -DARROW_BUILD_SHARED=OFF
      -DARROW_BUILD_UTILITIES=ON
      -DARROW_ACERO=ON
      -DARROW_COMPUTE=ON
      -DARROW_CSV=ON
      -DARROW_CXXFLAGS="-D_LIBCPP_DISABLE_AVAILABILITY"
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
