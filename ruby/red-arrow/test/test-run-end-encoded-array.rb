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

class RunEndEncodedArrayTest < Test::Unit::TestCase
  def build_array(values, run_ends, offset=0, length=8)
    data_type = Arrow::RunEndEncodedDataType.new(run_ends.value_data_type,
                                                 values.value_data_type)
    Arrow::RunEndEncodedArray.new(data_type, length, run_ends, values, offset)
  end

  def assert_converted(expected, array, conversion)
    split = [1, array.length].min
    chunks = [array.slice(0, 0),
              array.slice(0, split),
              array.slice(split, 0),
              array.slice(split, array.length - split),
              array.slice(array.length, 0)]
    case conversion
    when :values
      assert_equal(expected, array.values)
      assert_equal(expected, array.to_a)
    when :chunked_values
      assert_equal(expected, Arrow::ChunkedArray.new(chunks).values)
    else
      indices = Arrow::Int32Array.new((0...array.length).to_a)
      schema = Arrow::Schema.new(value: array.value_data_type, index: :int32)
      if conversion.to_s.start_with?("table_")
        target = Arrow::Table.new(schema,
                                  [Arrow::ChunkedArray.new(chunks),
                                   Arrow::ChunkedArray.new([indices])])
      else
        target = Arrow::RecordBatch.new(schema, array.length, [array, indices])
      end
      actual = if conversion.to_s.end_with?("each_raw_record")
                 target.each_raw_record.to_a
               else
                 target.raw_records
               end
      assert_equal(expected.each_with_index.collect {|value, i| [value, i]},
                   actual)
    end
  end

  sub_test_case("conversion") do
    data(:conversion,
         [:values, :chunked_values,
          :record_batch_raw_records, :record_batch_each_raw_record,
          :table_raw_records, :table_each_raw_record],
         keep: true)

    test("run ends and slices") do |data|
      values = Arrow::StringArray.new(["a", nil, "b"])
      expected = ["a", "a", nil, nil, nil, "b", "b", "b"]
      [Arrow::Int16Array, Arrow::Int32Array, Arrow::Int64Array].each do |type|
        run_ends = type.new([2, 5, 8])
        array = build_array(values, run_ends)
        [[0, 8], [1, 6], [2, 3], [3, 1], [5, 2], [0, 0], [4, 0], [8, 0]].each do |offset, length|
          assert_converted(expected.slice(offset, length),
                           array.slice(offset, length),
                           data[:conversion])
        end
        assert_converted(expected.slice(3, 4),
                         build_array(values, run_ends, 3, 4),
                         data[:conversion])
      end
    end

    test("64-bit logical offset") do |data|
      offset = 2 ** 32
      values = Arrow::StringArray.new(["a", nil, "b"])
      run_ends = Arrow::Int64Array.new([offset + 2, offset + 5, offset + 8])
      array = build_array(values, run_ends, offset + 1, 6)
      assert_converted(["a", nil, nil, nil, "b", "b"], array, data[:conversion])
    end

    test("empty") do |data|
      array = build_array(Arrow::StringArray.new([]), Arrow::Int16Array.new([]), 0, 0)
      assert_converted([], array, data[:conversion])
    end

    test("null values") do |data|
      array = build_array(Arrow::NullArray.new(3), Arrow::Int32Array.new([2, 5, 8]))
      assert_converted([nil] * 8, array, data[:conversion])
    end

    test("sliced physical children") do |data|
      values = Arrow::StringArray.new(["unused", "a", nil, "b"]).slice(1, 3)
      run_ends = Arrow::Int64Array.new([1, 2, 5, 8]).slice(1, 3)
      array = build_array(values, run_ends, 1, 6)
      assert_converted(["a", nil, nil, nil, "b", "b"], array, data[:conversion])
    end

    test("nested values") do |data|
      list_values = [[true, nil], nil, [false]]
      struct_values = [{"value" => true}, nil, {"value" => nil}]
      map_values = [{"a" => true, "b" => nil}, nil, {}]
      arrays = [
        [Arrow::ListArray.new([:list, :boolean], list_values), list_values],
        [Arrow::LargeListArray.new([:large_list, :boolean], list_values), list_values],
        [Arrow::FixedSizeListArray.new([:fixed_size_list, :boolean, 2],
                                       [[true, nil], nil, [false, true]]),
         [[true, nil], nil, [false, true]]],
        [Arrow::StructArray.new({value: :boolean}, struct_values), struct_values],
        [Arrow::MapArray.new([:map, :string, :boolean], map_values), map_values],
        [Arrow::StringArray.new(["a", nil, "b"]).dictionary_encode, ["a", nil, "b"]],
        [Arrow::SparseUnionArray.new(Arrow::Int8Array.new([0, 1, 0]),
                                     [Arrow::StringArray.new(["a", nil, "b"]),
                                      Arrow::BooleanArray.new([nil, nil, nil])]),
         ["a", nil, "b"]],
        [Arrow::DenseUnionArray.new(Arrow::Int8Array.new([0, 1, 0]),
                                    Arrow::Int32Array.new([0, 0, 1]),
                                    [Arrow::StringArray.new(["a", "b"]),
                                     Arrow::BooleanArray.new([nil])]),
         ["a", nil, "b"]],
      ]
      arrays.each do |values, expected|
        array = build_array(values, Arrow::Int32Array.new([2, 5, 8])).slice(1, 6)
        assert_converted([expected[0], expected[1], expected[1], expected[1],
                          expected[2], expected[2]], array, data[:conversion])
      end
    end

    test("run-end encoded child") do |data|
      values = build_array(Arrow::StringArray.new(["a", nil, "b"]),
                           Arrow::Int16Array.new([2, 5, 8])).slice(1, 6)
      type = values.value_data_type
      list_type = Arrow::ListDataType.new(Arrow::Field.new("item", type))
      large_list_type = Arrow::LargeListDataType.new(Arrow::Field.new("item", type))
      fixed_size_list_type = Arrow::FixedSizeListDataType.new(type, 2)
      struct_type = Arrow::StructDataType.new(value: type)
      dictionary_type = Arrow::DictionaryDataType.new(:int8, type, false)
      arrays = [
        [Arrow::ListArray.new(list_type, 2, Arrow::Buffer.new([0, 3, 6].pack("l*")),
                              values, nil, 0), [["a", nil, nil], [nil, "b", "b"]]],
        [Arrow::LargeListArray.new(large_list_type, 2,
                                   Arrow::Buffer.new([0, 3, 6].pack("q*")), values, nil, 0),
         [["a", nil, nil], [nil, "b", "b"]]],
        [Arrow::FixedSizeListArray.new(fixed_size_list_type, 3, values, nil, 0),
         [["a", nil], [nil, nil], ["b", "b"]]],
        [Arrow::StructArray.new(struct_type, 6, [values], nil, 0),
         ["a", nil, nil, nil, "b", "b"].collect {|v| {"value" => v}}],
        [Arrow::MapArray.new(Arrow::Int32Array.new([0, 3, 6]),
                             Arrow::StringArray.new(["a", "b", "c", "d", "e", "f"]),
                             values),
         [{"a" => "a", "b" => nil, "c" => nil}, {"d" => nil, "e" => "b", "f" => "b"}]],
        [Arrow::DictionaryArray.new(dictionary_type, Arrow::Int8Array.new([4, 1, 0]), values),
         ["b", nil, "a"]],
        [Arrow::SparseUnionArray.new(Arrow::Int8Array.new([0] * 6), [values]),
         ["a", nil, nil, nil, "b", "b"]],
        [Arrow::DenseUnionArray.new(Arrow::Int8Array.new([0] * 3),
                                    Arrow::Int32Array.new([4, 1, 0]), [values]),
         ["b", nil, "a"]],
        [build_array(values, Arrow::Int64Array.new([1, 2, 3, 4, 6, 8])),
         ["a", nil, nil, nil, "b", "b", "b", "b"]],
      ]
      arrays.each do |array, expected|
        assert_converted(expected, array, data[:conversion])
      end
    end
  end

  test("physical values") do
    values = Arrow::StringArray.new(["a", nil, "b"])
    array = build_array(values, Arrow::Int32Array.new([2, 5, 8]))
    assert_equal(values, array.values_raw)
    assert_equal(values, array.slice(3, 3).values_raw)
    assert_equal(Arrow::StringArray.new([nil, "b"]), array.slice(3, 3).logical_values)
  end
end
