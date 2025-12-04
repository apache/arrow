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

class TestMakeStructOptions < Test::Unit::TestCase
  include Helper::Buildable

  def setup
    @options = Arrow::MakeStructOptions.new
  end

  def test_field_names_property
    assert_equal([], @options.field_names)
    @options.field_names = ["a", "b", "c"]
    assert_equal(["a", "b", "c"], @options.field_names)
  end

  def test_make_struct_function
    a = build_int8_array([1, 2, 3])
    b = build_boolean_array([true, false, nil])
    args = [
      Arrow::ArrayDatum.new(a),
      Arrow::ArrayDatum.new(b),
    ]
    @options.field_names = ["a", "b"]
    make_struct_function = Arrow::Function.find("make_struct")
    result = make_struct_function.execute(args, @options).value

    expected = build_struct_array(
      [
        Arrow::Field.new("a", Arrow::Int8DataType.new),
        Arrow::Field.new("b", Arrow::BooleanDataType.new),
      ],
      [
        {"a" => 1, "b" => true},
        {"a" => 2, "b" => false},
        {"a" => 3, "b" => nil},
      ]
    )
    assert_equal(expected, result)
  end
end
