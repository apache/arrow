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

class TestListFlattenOptions < Test::Unit::TestCase
  include Helper::Buildable

  def setup
    @options = Arrow::ListFlattenOptions.new
  end

  def test_recursive_property
    assert do
      !@options.recursive?
    end
    @options.recursive = true
    assert do
      @options.recursive?
    end
  end

  def test_list_flatten_function_recursive
    list_data_type = Arrow::ListDataType.new(Arrow::Field.new("value", Arrow::Int8DataType.new))
    nested_list = build_list_array(list_data_type, [[[1, 2], [3]], [[4, 5]]])

    args = [
      Arrow::ArrayDatum.new(nested_list),
    ]
    list_flatten_function = Arrow::Function.find("list_flatten")

    @options.recursive = false
    result = list_flatten_function.execute(args, @options).value
    assert_equal(list_data_type,
                 result.value_data_type)
    assert_equal(build_list_array(Arrow::Int8DataType.new, [[1, 2], [3], [4, 5]]),
                 result)

    @options.recursive = true
    result = list_flatten_function.execute(args, @options).value
    assert_equal(Arrow::Int8DataType.new,
                 result.value_data_type)
    assert_equal(build_int8_array([1, 2, 3, 4, 5]),
                 result)
  end
end
