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

class TestRecordBatchBuilder < Test::Unit::TestCase
  include Helper::Buildable

  def setup
    @fields = [
      Arrow::Field.new("visible", Arrow::BooleanDataType.new),
      Arrow::Field.new("point", Arrow::Int32DataType.new),
    ]
    @schema = Arrow::Schema.new(@fields)
    @builder = Arrow::RecordBatchBuilder.new(@schema)
  end

  def test_initial_capacity
    @builder.initial_capacity = 128
    assert_equal(128, @builder.initial_capacity)
  end

  def test_schema
    assert_equal(@schema, @builder.schema)
  end

  def test_n_fields
    assert_equal(@fields.size, @builder.n_fields)
  end

  sub_test_case("#get_field") do
    def test_valid
      assert_equal(Arrow::BooleanArrayBuilder,
                   @builder.get_field(0).class)
    end

    def test_negative
      assert_equal(Arrow::Int32ArrayBuilder,
                   @builder.get_field(-1).class)
    end

    def test_too_negative
      assert_nil(@builder.get_field(-@fields.size - 1))
    end

    def test_too_large
      assert_nil(@builder.get_field(@fields.size))
    end
  end

  def test_flush
    arrays = {
      "visible" => build_boolean_array([true, false, true]),
      "point"   => build_int32_array([1, -1, 0]),
    }
    arrays.each_with_index do |(_, array), i|
      @builder.get_field(i).append_values(array.values, [])
    end
    assert_equal(build_record_batch(arrays),
                 @builder.flush)

    arrays = {
      "visible" => build_boolean_array([false, true]),
      "point"   => build_int32_array([10, -10]),
    }
    arrays.each_with_index do |(_, array), i|
      @builder.get_field(i).append_values(array.values, [])
    end
    assert_equal(build_record_batch(arrays),
                 @builder.flush)
  end
end
