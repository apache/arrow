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
  def test_is_null
    builder = Arrow::BooleanArrayBuilder.new
    builder.append_null
    builder.append(true)
    array = builder.finish
    assert_equal([true, false],
                 array.length.times.collect {|i| array.null?(i)})
  end

  def test_length
    builder = Arrow::BooleanArrayBuilder.new
    builder.append(true)
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

  def test_value_data_type
    builder = Arrow::BooleanArrayBuilder.new
    array = builder.finish
    assert_equal(Arrow::BooleanDataType.new, array.value_data_type)
  end

  def test_value_type
    builder = Arrow::BooleanArrayBuilder.new
    array = builder.finish
    assert_equal(Arrow::Type::BOOL, array.value_type)
  end

  def test_slice
    builder = Arrow::BooleanArrayBuilder.new
    builder.append(true)
    builder.append(false)
    builder.append(true)
    array = builder.finish
    sub_array = array.slice(1, 2)
    assert_equal([false, true],
                 sub_array.length.times.collect {|i| sub_array.get_value(i)})
  end
end
