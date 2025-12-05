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

class TestFileReader < Test::Unit::TestCase
  def setup
    Dir.mktmpdir do |tmp_dir|
      table = Arrow::Table.new(value: build_array)
      @path = File.join(tmp_dir, "data.arrow")
      table.save(@path)
      File.open(@path, "rb") do |input|
        @reader = ArrowFormat::FileReader.new(input)
        yield
        @reader = nil
      end
      GC.start
    end
  end

  def read
    @reader.to_a.collect do |record_batch|
      record_batch.to_h.tap do |hash|
        hash.each do |key, value|
          hash[key] = value.to_a
        end
      end
    end
  end

  sub_test_case("Null") do
    def build_array
      Arrow::NullArray.new(3)
    end

    def test_read
      assert_equal([{"value" => [nil, nil, nil]}],
                   read)
    end
  end

  sub_test_case("Boolean") do
    def build_array
      Arrow::BooleanArray.new([true, nil, false])
    end

    def test_read
      assert_equal([{"value" => [true, nil, false]}],
                   read)
    end
  end

  sub_test_case("Int8") do
    def build_array
      Arrow::Int8Array.new([-128, nil, 127])
    end

    def test_read
      assert_equal([{"value" => [-128, nil, 127]}],
                   read)
    end
  end

  sub_test_case("UInt8") do
    def build_array
      Arrow::UInt8Array.new([0, nil, 255])
    end

    def test_read
      assert_equal([{"value" => [0, nil, 255]}],
                   read)
    end
  end

  sub_test_case("Binary") do
    def build_array
      Arrow::BinaryArray.new(["Hello".b, nil, "World".b])
    end

    def test_read
      assert_equal([{"value" => ["Hello".b, nil, "World".b]}],
                   read)
    end
  end

  sub_test_case("UTF8") do
    def build_array
      Arrow::StringArray.new(["Hello", nil, "World"])
    end

    def test_read
      assert_equal([{"value" => ["Hello", nil, "World"]}],
                   read)
    end
  end
end
