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

class TestWriteOptions < Test::Unit::TestCase
  def setup
    @options = Arrow::WriteOptions.new
  end

  sub_test_case("allow-64bit") do
    def test_default
      assert do
        not @options.allow_64bit?
      end
    end

    def test_accessor
      @options.allow_64bit = true
      assert do
        @options.allow_64bit?
      end
    end
  end

  sub_test_case("max-recursion-depth") do
    def test_default
      assert_equal(64, @options.max_recursion_depth)
    end

    def test_accessor
      @options.max_recursion_depth = 29
      assert_equal(29, @options.max_recursion_depth)
    end
  end


  sub_test_case("alignment") do
    def test_default
      assert_equal(8, @options.alignment)
    end

    def test_accessor
      @options.alignment = 64
      assert_equal(64, @options.alignment)
    end
  end

  sub_test_case("write-legacy-ipc-format") do
    def test_default
      assert do
        not @options.write_legacy_ipc_format?
      end
    end

    def test_accessor
      @options.write_legacy_ipc_format = true
      assert do
        @options.write_legacy_ipc_format?
      end
    end
  end

  sub_test_case("compression") do
    def test_default
      assert_equal(Arrow::CompressionType::UNCOMPRESSED,
                   @options.compression)
    end

    def test_accessor
      @options.compression = :zstd
      assert_equal(Arrow::CompressionType::ZSTD,
                   @options.compression)
    end
  end

  sub_test_case("compression-level") do
    def test_default
      assert_equal(-(2 ** 31), @options.compression_level)
    end

    def test_accessor
      @options.compression_level = 8
      assert_equal(8, @options.compression_level)
    end
  end

  sub_test_case("use-threads") do
    def test_default
      assert do
        @options.use_threads?
      end
    end

    def test_accessor
      @options.use_threads = false
      assert do
        not @options.use_threads?
      end
    end
  end
end
