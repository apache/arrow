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

  sub_test_case("Int16") do
    def build_array
      Arrow::Int16Array.new([-32768, nil, 32767])
    end

    def test_read
      assert_equal([{"value" => [-32768, nil, 32767]}],
                   read)
    end
  end

  sub_test_case("UInt16") do
    def build_array
      Arrow::UInt16Array.new([0, nil, 65535])
    end

    def test_read
      assert_equal([{"value" => [0, nil, 65535]}],
                   read)
    end
  end

  sub_test_case("Int32") do
    def build_array
      Arrow::Int32Array.new([-2147483648, nil, 2147483647])
    end

    def test_read
      assert_equal([{"value" => [-2147483648, nil, 2147483647]}],
                   read)
    end
  end

  sub_test_case("UInt32") do
    def build_array
      Arrow::UInt32Array.new([0, nil, 4294967295])
    end

    def test_read
      assert_equal([{"value" => [0, nil, 4294967295]}],
                   read)
    end
  end

  sub_test_case("Int64") do
    def build_array
      Arrow::Int64Array.new([
                              -9223372036854775808,
                              nil,
                              9223372036854775807
                            ])
    end

    def test_read
      assert_equal([
                     {
                       "value" => [
                         -9223372036854775808,
                         nil,
                         9223372036854775807
                       ]
                     }
                   ],
                   read)
    end
  end

  sub_test_case("UInt64") do
    def build_array
      Arrow::UInt64Array.new([0, nil, 18446744073709551615])
    end

    def test_read
      assert_equal([{"value" => [0, nil, 18446744073709551615]}],
                   read)
    end
  end

  sub_test_case("Float32") do
    def build_array
      Arrow::FloatArray.new([-0.5, nil, 0.5])
    end

    def test_read
      assert_equal([{"value" => [-0.5, nil, 0.5]}],
                   read)
    end
  end

  sub_test_case("Float64") do
    def build_array
      Arrow::DoubleArray.new([-0.5, nil, 0.5])
    end

    def test_read
      assert_equal([{"value" => [-0.5, nil, 0.5]}],
                   read)
    end
  end

  sub_test_case("Date32") do
    def setup(&block)
      @date_2017_08_28 = 17406
      @date_2025_12_09 = 20431
      super(&block)
    end

    def build_array
      Arrow::Date32Array.new([@date_2017_08_28, nil, @date_2025_12_09])
    end

    def test_read
      assert_equal([{"value" => [@date_2017_08_28, nil, @date_2025_12_09]}],
                   read)
    end
  end

  sub_test_case("Date64") do
    def setup(&block)
      @date_2017_08_28_00_00_00 = 1503878400000
      @date_2025_12_09_00_00_00 = 1765324800000
      super(&block)
    end

    def build_array
      Arrow::Date64Array.new([
                               @date_2017_08_28_00_00_00,
                               nil,
                               @date_2025_12_09_00_00_00,
                             ])
    end

    def test_read
      assert_equal([
                     {
                       "value" => [
                         @date_2017_08_28_00_00_00,
                         nil,
                         @date_2025_12_09_00_00_00,
                       ],
                     },
                   ],
                   read)
    end
  end

  sub_test_case("Time32(:second)") do
    def setup(&block)
      @time_00_00_10 = 10
      @time_00_01_10 = 60 + 10
      super(&block)
    end

    def build_array
      Arrow::Time32Array.new(:second, [@time_00_00_10, nil, @time_00_01_10])
    end

    def test_read
      assert_equal([{"value" => [@time_00_00_10, nil, @time_00_01_10]}],
                   read)
    end
  end

  sub_test_case("Time32(:millisecond)") do
    def setup(&block)
      @time_00_00_10_000 = 10 * 1000
      @time_00_01_10_000 = (60 + 10) * 1000
      super(&block)
    end

    def build_array
      Arrow::Time32Array.new(:milli,
                             [@time_00_00_10_000, nil, @time_00_01_10_000])
    end

    def test_read
      assert_equal([{"value" => [@time_00_00_10_000, nil, @time_00_01_10_000]}],
                   read)
    end
  end

  sub_test_case("Time64(:microsecond)") do
    def setup(&block)
      @time_00_00_10_000_000 = 10 * 1_000_000
      @time_00_01_10_000_000 = (60 + 10) * 1_000_000
      super(&block)
    end

    def build_array
      Arrow::Time64Array.new(:micro,
                             [
                               @time_00_00_10_000_000,
                               nil,
                               @time_00_01_10_000_000,
                             ])
    end

    def test_read
      assert_equal([
                     {
                       "value" => [
                         @time_00_00_10_000_000,
                         nil,
                         @time_00_01_10_000_000,
                       ],
                     },
                   ],
                   read)
    end
  end

  sub_test_case("Time64(:nanosecond)") do
    def setup(&block)
      @time_00_00_10_000_000_000 = 10 * 1_000_000_000
      @time_00_01_10_000_000_000 = (60 + 10) * 1_000_000_000
      super(&block)
    end

    def build_array
      Arrow::Time64Array.new(:nano,
                             [
                               @time_00_00_10_000_000_000,
                               nil,
                               @time_00_01_10_000_000_000,
                             ])
    end

    def test_read
      assert_equal([
                     {
                       "value" => [
                         @time_00_00_10_000_000_000,
                         nil,
                         @time_00_01_10_000_000_000,
                       ],
                     },
                   ],
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

  sub_test_case("LargeBinary") do
    def build_array
      Arrow::LargeBinaryArray.new(["Hello".b, nil, "World".b])
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

  sub_test_case("FixedSizeBinary") do
    def build_array
      data_type = Arrow::FixedSizeBinaryDataType.new(4)
      Arrow::FixedSizeBinaryArray.new(data_type, ["0124".b, nil, "abcd".b])
    end

    def test_read
      assert_equal([{"value" => ["0124".b, nil, "abcd".b]}],
                   read)
    end
  end

  sub_test_case("List") do
    def build_array
      data_type = Arrow::ListDataType.new(name: "count", type: :int8)
      Arrow::ListArray.new(data_type, [[-128, 127], nil, [-1, 0, 1]])
    end

    def test_read
      assert_equal([{"value" => [[-128, 127], nil, [-1, 0, 1]]}],
                   read)
    end
  end

  sub_test_case("LargeList") do
    def build_array
      data_type = Arrow::LargeListDataType.new(name: "count", type: :int8)
      Arrow::LargeListArray.new(data_type, [[-128, 127], nil, [-1, 0, 1]])
    end

    def test_read
      assert_equal([{"value" => [[-128, 127], nil, [-1, 0, 1]]}],
                   read)
    end
  end

  sub_test_case("Struct") do
    def build_array
      data_type = Arrow::StructDataType.new(count: :int8,
                                            visible: :boolean)
      Arrow::StructArray.new(data_type, [[-128, nil], nil, [nil, true]])
    end

    def test_read
      assert_equal([
                     {
                       "value" => [
                         [-128, nil],
                         nil,
                         [nil, true],
                       ],
                     },
                   ],
                   read)
    end
  end

  sub_test_case("DenseUnion") do
    def build_array
      fields = [
        Arrow::Field.new("number", :int8),
        Arrow::Field.new("text", :string),
      ]
      type_ids = [11, 13]
      data_type = Arrow::DenseUnionDataType.new(fields, type_ids)
      types = Arrow::Int8Array.new([11, 13, 11, 13, 13])
      value_offsets = Arrow::Int32Array.new([0, 0, 1, 1, 2])
      children = [
        Arrow::Int8Array.new([1, nil]),
        Arrow::StringArray.new(["a", "b", "c"])
      ]
      Arrow::DenseUnionArray.new(data_type,
                                 types,
                                 value_offsets,
                                 children)
    end

    def test_read
      assert_equal([{"value" => [1, "a", nil, "b", "c"]}],
                   read)
    end
  end

  sub_test_case("SparseUnion") do
    def build_array
      fields = [
        Arrow::Field.new("number", :int8),
        Arrow::Field.new("text", :string),
      ]
      type_ids = [11, 13]
      data_type = Arrow::SparseUnionDataType.new(fields, type_ids)
      types = Arrow::Int8Array.new([11, 13, 11, 13, 11])
      children = [
        Arrow::Int8Array.new([1, nil, nil, nil, 5]),
        Arrow::StringArray.new([nil, "b", nil, "d", nil])
      ]
      Arrow::SparseUnionArray.new(data_type, types, children)
    end

    def test_read
      assert_equal([{"value" => [1, "b", nil, "d", 5]}],
                   read)
    end
  end

  sub_test_case("Map") do
    def build_array
      data_type = Arrow::MapDataType.new(:string, :int8)
      Arrow::MapArray.new(data_type,
                          [
                            {"a" => -128, "b" => 127},
                            nil,
                            {"c" => nil},
                          ])
    end

    def test_read
      assert_equal([
                     {
                       "value" => [
                         {"a" => -128, "b" => 127},
                         nil,
                         {"c" => nil},
                       ],
                     },
                   ],
                   read)
    end
  end
end
