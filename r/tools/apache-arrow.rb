# https://github.com/autobrew/homebrew-core/blob/master/Formula/apache-arrow.rb
class ApacheArrow < Formula
  desc "Columnar in-memory analytics layer designed to accelerate big data"
  homepage "https://arrow.apache.org/"
  url "https://archive.apache.org/dist/arrow/arrow-0.14.1/apache-arrow-0.14.1.tar.gz"
  sha256 "9948ddb6d4798b51552d0dca3252dd6e3a7d0f9702714fc6f5a1b59397ce1d28"
  head "https://github.com/apache/arrow.git"

  bottle do
    cellar :any
    sha256 "b00e518cda38f90ecefaf542d4c030a966eae50623b057e9edb6fbe93fc3f4ab" => :el_capitan_or_later
    root_url "https://jeroen.github.io/bottles"
  end

  depends_on "cmake" => :build
  depends_on "flatbuffers" => :build
  depends_on "double-conversion"
  depends_on "boost"
  depends_on "lz4"
  depends_on "thrift"
  depends_on "snappy"

  def install
    ENV.cxx11
    args = %W[
      -DARROW_PARQUET=ON
      -DARROW_PLASMA=OFF
      -DARROW_HDFS=OFF
      -DARROW_BUILD_TESTS=OFF
      -DARROW_TEST_LINKAGE="static"
      -DARROW_BUILD_SHARED=OFF
      -DARROW_JEMALLOC=OFF
      -DARROW_WITH_BROTLI=OFF
      -DARROW_USE_GLOG=OFF
      -DARROW_PYTHON=OFF
      -DARROW_WITH_ZSTD=OFF
      -DARROW_WITH_SNAPPY=ON
      -DARROW_BUILD_UTILITIES=ON
      -DPARQUET_BUILD_EXECUTABLES=ON
      -DFLATBUFFERS_HOME=#{Formula["flatbuffers"].prefix}
      -DLZ4_HOME=#{Formula["lz4"].prefix}
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
