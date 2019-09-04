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

class TestArray < Test::Unit::TestCase
  include Helper::Buildable

  def test_equal
    assert_equal(build_boolean_array([true, false]),
                 build_boolean_array([true, false]))
  end

  def test_equal_approx
    array1 = build_double_array([1.1, 2.2 + Float::EPSILON * 10])
    array2 = build_double_array([1.1, 2.2])
    assert do
      array1.equal_approx(array2)
    end
  end

  def test_equal_range
    array1 = build_int32_array([1, 2, 3, 4, 5])
    array2 = build_int32_array([-2, -1, 0, 1, 2, 3, 4, 999])
    assert do
      array1.equal_range(1, array2, 4, 3)
    end
  end

  def test_is_null
    builder = Arrow::BooleanArrayBuilder.new
    builder.append_null
    builder.append_value(true)
    array = builder.finish
    assert_equal([true, false],
                 array.length.times.collect {|i| array.null?(i)})
  end

  def test_is_valid
    builder = Arrow::BooleanArrayBuilder.new
    builder.append_null
    builder.append_value(true)
    array = builder.finish
    assert_equal([false, true],
                 array.length.times.collect {|i| array.valid?(i)})
  end

  def test_length
    builder = Arrow::BooleanArrayBuilder.new
    builder.append_value(true)
    array = builder.finish
    assert_equal(1, array.length)
  end

  def test_n_nulls
    builder = Arrow::BooleanArrayBuilder.new
    builder.append_null
    builder.append_null
    array = builder.finish
    assert_equal(2, array.n_nulls)
  end

  def test_null_bitmap
    builder = Arrow::BooleanArrayBuilder.new
    builder.append_null
    builder.append_value(true)
    builder.append_value(false)
    builder.append_null
    builder.append_value(false)
    array = builder.finish
    assert_equal(0b10110, array.null_bitmap.data.to_s.unpack("c*")[0])
  end

  def test_value_data_type
    builder = Arrow::BooleanArrayBuilder.new
    array = builder.finish
    assert_equal(Arrow::BooleanDataType.new, array.value_data_type)
  end

  def test_value_type
    builder = Arrow::BooleanArrayBuilder.new
    array = builder.finish
    assert_equal(Arrow::Type::BOOLEAN, array.value_type)
  end

  def test_slice
    builder = Arrow::BooleanArrayBuilder.new
    builder.append_value(true)
    builder.append_value(false)
    builder.append_value(true)
    array = builder.finish
    sub_array = array.slice(1, 2)
    assert_equal([false, true],
                 sub_array.length.times.collect {|i| sub_array.get_value(i)})
  end

  def test_to_s
    assert_equal(<<-CONTENT.chomp, build_boolean_array([true, false, true]).to_s)
[
  true,
  false,
  true
]
    CONTENT
  end

  sub_test_case("#view") do
    def test_valid
      assert_equal(build_float_array([0.0, 1.5, -2.5, nil]),
                   build_int32_array([0, 1069547520, -1071644672, nil]).view(Arrow::FloatDataType.new))
    end

    def test_invalid
      message = "[array][view]: Invalid: " +
                "Can't view array of type int16 as int8: incompatible layouts"
      error = assert_raise(Arrow::Error::Invalid) do
        build_int16_array([0, -1, 3]).view(Arrow::Int8DataType.new)
      end
      assert_equal(message, error.message.lines.first.chomp)
    end
  end

  sub_test_case("#diff_unified") do
    def test_no_diff
      array = build_string_array(["Start", "Shutdown", "Reboot"])
      other_array = build_string_array(["Start", "Shutdown", "Reboot"])
      assert_nil(array.diff_unified(other_array))
    end

    def test_diff
      array = build_string_array(["Start", "Shutdown", "Reboot"])
      other_array = build_string_array(["Start", "Shutdonw", "Reboot"])
      assert_equal(<<-STRING.chomp, array.diff_unified(other_array))

@@ -1, +1 @@
-"Shutdown"
+"Shutdonw"

      STRING
    end

    def test_different_type
      array = build_string_array(["Start", "Shutdown", "Reboot"])
      other_array = build_int8_array([2, 3, 6, 10])
      assert_equal("# Array types differed: string vs int8",
                   array.diff_unified(other_array))
    end
  end
end
