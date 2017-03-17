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

class TestColumn < Test::Unit::TestCase
  include Helper::Buildable

  sub_test_case(".new") do
    def test_array
      field = Arrow::Field.new("enabled", Arrow::BooleanDataType.new)
      array = build_boolean_array([true])
      column = Arrow::Column.new(field, array)
      assert_equal(1, column.length)
    end

    def test_chunked_array
      field = Arrow::Field.new("enabled", Arrow::BooleanDataType.new)
      chunks = [
        build_boolean_array([true]),
        build_boolean_array([false, true]),
      ]
      chunked_array = Arrow::ChunkedArray.new(chunks)
      column = Arrow::Column.new(field, chunked_array)
      assert_equal(3, column.length)
    end
  end

  def test_length
    field = Arrow::Field.new("enabled", Arrow::BooleanDataType.new)
    array = build_boolean_array([true, false])
    column = Arrow::Column.new(field, array)
    assert_equal(2, column.length)
  end

  def test_n_nulls
    field = Arrow::Field.new("enabled", Arrow::BooleanDataType.new)
    array = build_boolean_array([true, nil, nil])
    column = Arrow::Column.new(field, array)
    assert_equal(2, column.n_nulls)
  end

  def test_field
    field = Arrow::Field.new("enabled", Arrow::BooleanDataType.new)
    array = build_boolean_array([true])
    column = Arrow::Column.new(field, array)
    assert_equal("enabled", column.field.name)
  end

  def test_name
    field = Arrow::Field.new("enabled", Arrow::BooleanDataType.new)
    array = build_boolean_array([true])
    column = Arrow::Column.new(field, array)
    assert_equal("enabled", column.name)
  end

  def test_data_type
    field = Arrow::Field.new("enabled", Arrow::BooleanDataType.new)
    array = build_boolean_array([true])
    column = Arrow::Column.new(field, array)
    assert_equal("bool", column.data_type.to_s)
  end

  def test_data
    field = Arrow::Field.new("enabled", Arrow::BooleanDataType.new)
    chunks = [
      build_boolean_array([true]),
      build_boolean_array([false, true]),
    ]
    chunked_array = Arrow::ChunkedArray.new(chunks)
    column = Arrow::Column.new(field, chunked_array)
    assert_equal(3, column.data.length)
  end
end
