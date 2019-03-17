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

module ArrayBuilderAppendValuesTests
  def test_empty
    require_gi(1, 42, 0)
    builder = create_builder
    builder.append_values([])
    assert_equal(build_array([]),
                 builder.finish)
  end

  def test_values_only
    require_gi(1, 42, 0)
    builder = create_builder
    builder.append_values(sample_values)
    assert_equal(build_array(sample_values),
                 builder.finish)
  end

  def test_with_is_valids
    builder = create_builder
    builder.append_values(sample_values, [true, true, false])
    sample_values_with_null = sample_values
    sample_values_with_null[2] = nil
    assert_equal(build_array(sample_values_with_null),
                 builder.finish)
  end

  def test_with_large_is_valids
    builder = create_builder
    n = 10000
    large_sample_values = sample_values * n
    large_is_valids = [true, true, false] * n
    builder.append_values(large_sample_values, large_is_valids)
    sample_values_with_null = sample_values
    sample_values_with_null[2] = nil
    large_sample_values_with_null = sample_values_with_null * n
    assert_equal(build_array(large_sample_values_with_null),
                 builder.finish)
  end

  def test_mismatch_length
    builder = create_builder
    message = "[#{builder_class_name}][append-values]: " +
      "values length and is_valids length must be equal: <3> != <2>"
    assert_raise(Arrow::Error::Invalid.new(message)) do
      builder.append_values(sample_values, [true, true])
    end
  end
end

module ArrayBuilderAppendNullsTests
  def test_zero
    builder = create_builder
    builder.append_nulls(0)
    assert_equal(build_array([]),
                 builder.finish)
  end

  def test_positive
    builder = create_builder
    builder.append_nulls(3)
    assert_equal(build_array([nil, nil, nil]),
                 builder.finish)
  end

  def test_negative
    builder = create_builder
    message = "[#{builder_class_name}][append-nulls]: " +
      "the number of nulls must be 0 or larger: <-1>"
    assert_raise(Arrow::Error::Invalid.new(message)) do
      builder.append_nulls(-1)
    end
  end
end

module ArrayBuilderValueTypeTests
  def test_value_data_type
    assert_equal(value_data_type,
                 build_array(sample_values).value_data_type)
  end

  def test_value_type
    assert_equal(value_data_type.id,
                 build_array(sample_values).value_type)
  end
end

class TestArrayBuilder < Test::Unit::TestCase
  include Helper::Buildable
  include Helper::Omittable

  def setup
    require_gi_bindings(3, 1, 9)
  end

  def build_array(values)
    super(create_builder, values)
  end

  sub_test_case("NullArrayBuilder") do
    def create_builder
      Arrow::NullArrayBuilder.new
    end

    def value_data_type
      Arrow::NullDataType.new
    end

    def builder_class_name
      "null-array-builder"
    end

    def sample_values
      [nil, nil, nil]
    end

    sub_test_case("value type") do
      include ArrayBuilderValueTypeTests
    end

    test("#append_null") do
      builder = create_builder
      builder.append_null
      assert_equal(build_array([nil]),
                   builder.finish)
    end

    sub_test_case("#append_nulls") do
      include ArrayBuilderAppendNullsTests
    end
  end

  sub_test_case("BooleanArrayBuilder") do
    def create_builder
      Arrow::BooleanArrayBuilder.new
    end

    def value_data_type
      Arrow::BooleanDataType.new
    end

    def builder_class_name
      "boolean-array-builder"
    end

    def sample_values
      [true, false, true]
    end

    sub_test_case("value type") do
      include ArrayBuilderValueTypeTests
    end

    sub_test_case("#append_values") do
      include ArrayBuilderAppendValuesTests
    end

    sub_test_case("#append_nulls") do
      include ArrayBuilderAppendNullsTests
    end
  end

  sub_test_case("IntArrayBuilder") do
    def create_builder
      Arrow::IntArrayBuilder.new
    end

    def value_data_type
      Arrow::Int8DataType.new
    end

    def builder_class_name
      "int-array-builder"
    end

    def sample_values
      [1, -2, 3]
    end

    sub_test_case("value type") do
      include ArrayBuilderValueTypeTests
    end

    sub_test_case("#append_values") do
      include ArrayBuilderAppendValuesTests
    end

    sub_test_case("#append_nulls") do
      include ArrayBuilderAppendNullsTests
    end
  end

  sub_test_case("UIntArrayBuilder") do
    def create_builder
      Arrow::UIntArrayBuilder.new
    end

    def value_data_type
      Arrow::UInt8DataType.new
    end

    def builder_class_name
      "uint-array-builder"
    end

    def sample_values
      [1, 2, 3]
    end

    sub_test_case("value type") do
      include ArrayBuilderValueTypeTests
    end

    sub_test_case("#append_values") do
      include ArrayBuilderAppendValuesTests
    end

    sub_test_case("#append_nulls") do
      include ArrayBuilderAppendNullsTests
    end
  end

  sub_test_case("Int8ArrayBuilder") do
    def create_builder
      Arrow::Int8ArrayBuilder.new
    end

    def value_data_type
      Arrow::Int8DataType.new
    end

    def builder_class_name
      "int8-array-builder"
    end

    def sample_values
      [1, -2, 3]
    end

    sub_test_case("value type") do
      include ArrayBuilderValueTypeTests
    end

    sub_test_case("#append_values") do
      include ArrayBuilderAppendValuesTests
    end

    sub_test_case("#append_nulls") do
      include ArrayBuilderAppendNullsTests
    end
  end

  sub_test_case("UInt8ArrayBuilder") do
    def create_builder
      Arrow::UInt8ArrayBuilder.new
    end

    def value_data_type
      Arrow::UInt8DataType.new
    end

    def builder_class_name
      "uint8-array-builder"
    end

    def sample_values
      [1, 2, 3]
    end

    sub_test_case("value type") do
      include ArrayBuilderValueTypeTests
    end

    sub_test_case("#append_values") do
      include ArrayBuilderAppendValuesTests
    end

    sub_test_case("#append_nulls") do
      include ArrayBuilderAppendNullsTests
    end
  end

  sub_test_case("Int16ArrayBuilder") do
    def create_builder
      Arrow::Int16ArrayBuilder.new
    end

    def value_data_type
      Arrow::Int16DataType.new
    end

    def builder_class_name
      "int16-array-builder"
    end

    def sample_values
      [1, -2, 3]
    end

    sub_test_case("value type") do
      include ArrayBuilderValueTypeTests
    end

    sub_test_case("#append_values") do
      include ArrayBuilderAppendValuesTests
    end

    sub_test_case("#append_nulls") do
      include ArrayBuilderAppendNullsTests
    end
  end

  sub_test_case("UInt16ArrayBuilder") do
    def create_builder
      Arrow::UInt16ArrayBuilder.new
    end

    def value_data_type
      Arrow::UInt16DataType.new
    end

    def builder_class_name
      "uint16-array-builder"
    end

    def sample_values
      [1, 2, 3]
    end

    sub_test_case("value type") do
      include ArrayBuilderValueTypeTests
    end

    sub_test_case("#append_values") do
      include ArrayBuilderAppendValuesTests
    end

    sub_test_case("#append_nulls") do
      include ArrayBuilderAppendNullsTests
    end
  end

  sub_test_case("Int32ArrayBuilder") do
    def create_builder
      Arrow::Int32ArrayBuilder.new
    end

    def value_data_type
      Arrow::Int32DataType.new
    end

    def builder_class_name
      "int32-array-builder"
    end

    def sample_values
      [1, -2, 3]
    end

    sub_test_case("value type") do
      include ArrayBuilderValueTypeTests
    end

    sub_test_case("#append_values") do
      include ArrayBuilderAppendValuesTests
    end

    sub_test_case("#append_nulls") do
      include ArrayBuilderAppendNullsTests
    end
  end

  sub_test_case("UInt32ArrayBuilder") do
    def create_builder
      Arrow::UInt32ArrayBuilder.new
    end

    def value_data_type
      Arrow::UInt32DataType.new
    end

    def builder_class_name
      "uint32-array-builder"
    end

    def sample_values
      [1, 2, 3]
    end

    sub_test_case("value type") do
      include ArrayBuilderValueTypeTests
    end

    sub_test_case("#append_values") do
      include ArrayBuilderAppendValuesTests
    end

    sub_test_case("#append_nulls") do
      include ArrayBuilderAppendNullsTests
    end
  end

  sub_test_case("Int64ArrayBuilder") do
    def create_builder
      Arrow::Int64ArrayBuilder.new
    end

    def value_data_type
      Arrow::Int64DataType.new
    end

    def builder_class_name
      "int64-array-builder"
    end

    def sample_values
      [1, -2, 3]
    end

    sub_test_case("value type") do
      include ArrayBuilderValueTypeTests
    end

    sub_test_case("#append_values") do
      include ArrayBuilderAppendValuesTests
    end

    sub_test_case("#append_nulls") do
      include ArrayBuilderAppendNullsTests
    end
  end

  sub_test_case("UInt64ArrayBuilder") do
    def create_builder
      Arrow::UInt64ArrayBuilder.new
    end

    def value_data_type
      Arrow::UInt64DataType.new
    end

    def builder_class_name
      "uint64-array-builder"
    end

    def sample_values
      [1, 2, 3]
    end

    sub_test_case("value type") do
      include ArrayBuilderValueTypeTests
    end

    sub_test_case("#append_values") do
      include ArrayBuilderAppendValuesTests
    end

    sub_test_case("#append_nulls") do
      include ArrayBuilderAppendNullsTests
    end
  end

  sub_test_case("FloatArrayBuilder") do
    def create_builder
      Arrow::FloatArrayBuilder.new
    end

    def value_data_type
      Arrow::FloatDataType.new
    end

    def builder_class_name
      "float-array-builder"
    end

    def sample_values
      [1.1, -2.2, 3.3]
    end

    sub_test_case("value type") do
      include ArrayBuilderValueTypeTests
    end

    sub_test_case("#append_values") do
      include ArrayBuilderAppendValuesTests
    end

    sub_test_case("#append_nulls") do
      include ArrayBuilderAppendNullsTests
    end
  end

  sub_test_case("DoubleArrayBuilder") do
    def create_builder
      Arrow::DoubleArrayBuilder.new
    end

    def value_data_type
      Arrow::DoubleDataType.new
    end

    def builder_class_name
      "double-array-builder"
    end

    def sample_values
      [1.1, -2.2, 3.3]
    end

    sub_test_case("value type") do
      include ArrayBuilderValueTypeTests
    end

    sub_test_case("#append_values") do
      include ArrayBuilderAppendValuesTests
    end

    sub_test_case("#append_nulls") do
      include ArrayBuilderAppendNullsTests
    end
  end

  sub_test_case("Date32ArrayBuilder") do
    def create_builder
      Arrow::Date32ArrayBuilder.new
    end

    def value_data_type
      Arrow::Date32DataType.new
    end

    def builder_class_name
      "date32-array-builder"
    end

    def sample_values
      [
        0,     # epoch
        17406, # 2017-08-28
        17427, # 2017-09-18
      ]
    end

    sub_test_case("value type") do
      include ArrayBuilderValueTypeTests
    end

    sub_test_case("#append_values") do
      include ArrayBuilderAppendValuesTests
    end

    sub_test_case("#append_nulls") do
      include ArrayBuilderAppendNullsTests
    end
  end

  sub_test_case("Date64ArrayBuilder") do
    def create_builder
      Arrow::Date64ArrayBuilder.new
    end

    def value_data_type
      Arrow::Date64DataType.new
    end

    def builder_class_name
      "date64-array-builder"
    end

    def sample_values
      [
        -315619200,    # 1960-01-01T00:00:00Z
        0,             # epoch
        1503878400000, # 2017-08-28T00:00:00Z
      ]
    end

    sub_test_case("value type") do
      include ArrayBuilderValueTypeTests
    end

    sub_test_case("#append_values") do
      include ArrayBuilderAppendValuesTests
    end

    sub_test_case("#append_nulls") do
      include ArrayBuilderAppendNullsTests
    end
  end

  sub_test_case("TimestampArrayBuilder") do
    def create_builder
      data_type = Arrow::TimestampDataType.new(:milli)
      Arrow::TimestampArrayBuilder.new(data_type)
    end

    def value_data_type
      Arrow::TimestampDataType.new(:milli)
    end

    def builder_class_name
      "timestamp-array-builder"
    end

    def sample_values
      [
        0,             # epoch
        1504953190854, # 2017-09-09T10:33:10.854Z
        1505660812942, # 2017-09-17T15:06:52.942Z
      ]
    end

    sub_test_case("value type") do
      include ArrayBuilderValueTypeTests
    end

    sub_test_case("#append_values") do
      include ArrayBuilderAppendValuesTests
    end

    sub_test_case("#append_nulls") do
      include ArrayBuilderAppendNullsTests
    end
  end

  sub_test_case("Time32ArrayBuilder") do
    def create_builder
      data_type = Arrow::Time32DataType.new(:second)
      Arrow::Time32ArrayBuilder.new(data_type)
    end

    def value_data_type
      Arrow::Time32DataType.new(:second)
    end

    def builder_class_name
      "time32-array-builder"
    end

    def sample_values
      [
        0,                # midnight
        60 * 10,          # 00:10:00
        60 * 60 * 2 + 30, # 02:00:30
      ]
    end

    sub_test_case("value type") do
      include ArrayBuilderValueTypeTests
    end

    sub_test_case("#append_values") do
      include ArrayBuilderAppendValuesTests
    end

    sub_test_case("#append_nulls") do
      include ArrayBuilderAppendNullsTests
    end
  end

  sub_test_case("Time64ArrayBuilder") do
    def create_builder
      data_type = Arrow::Time64DataType.new(:micro)
      Arrow::Time64ArrayBuilder.new(data_type)
    end

    def value_data_type
      Arrow::Time64DataType.new(:micro)
    end

    def builder_class_name
      "time64-array-builder"
    end

    def sample_values
      [
        0,                                # midnight
        60 * 10 * 1000 * 1000,            # 00:10:00.000000
        (60 * 60 * 2 + 30) * 1000 * 1000, # 02:00:30.000000
      ]
    end

    sub_test_case("value type") do
      include ArrayBuilderValueTypeTests
    end

    sub_test_case("#append_values") do
      include ArrayBuilderAppendValuesTests
    end

    sub_test_case("#append_nulls") do
      include ArrayBuilderAppendNullsTests
    end
  end

  sub_test_case("StringArrayBuilder") do
    def create_builder
      Arrow::StringArrayBuilder.new
    end

    def value_data_type
      Arrow::StringDataType.new
    end

    def builder_class_name
      "string-array-builder"
    end

    def sample_values
      [
        "hello",
        "world!!",
        "",
      ]
    end

    sub_test_case("value type") do
      include ArrayBuilderValueTypeTests
    end

    sub_test_case("#append_values") do
      include ArrayBuilderAppendValuesTests
    end
  end
end
