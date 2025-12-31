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

class TestPivotWiderOptions < Test::Unit::TestCase
  include Helper::Buildable

  def setup
    @options = Arrow::PivotWiderOptions.new
  end

  def test_key_names_property
    assert_equal([], @options.key_names)
    @options.key_names = ["height", "width"]
    assert_equal(["height", "width"], @options.key_names)
  end

  def test_unexpected_key_behavior_property
    assert_equal(Arrow::PivotWiderUnexpectedKeyBehavior::IGNORE, @options.unexpected_key_behavior)
    @options.unexpected_key_behavior = :raise
    assert_equal(Arrow::PivotWiderUnexpectedKeyBehavior::RAISE, @options.unexpected_key_behavior)
    @options.unexpected_key_behavior = :ignore
    assert_equal(Arrow::PivotWiderUnexpectedKeyBehavior::IGNORE, @options.unexpected_key_behavior)
  end

  def test_pivot_wider_function
    keys = build_string_array(["height", "width", "depth"])
    values = build_int32_array([10, 20, 30])
    args = [
      Arrow::ArrayDatum.new(keys),
      Arrow::ArrayDatum.new(values),
    ]
    @options.key_names = ["height", "width"]
    pivot_wider_function = Arrow::Function.find("pivot_wider")
    result = pivot_wider_function.execute(args, @options).value
    fields = [
      Arrow::Field.new("height", Arrow::Int32DataType.new),
      Arrow::Field.new("width", Arrow::Int32DataType.new),
    ]
    data_type = Arrow::StructDataType.new(fields)
    expected = Arrow::StructScalar.new(data_type, [
      Arrow::Int32Scalar.new(10),
      Arrow::Int32Scalar.new(20),
    ])
    assert_equal(expected, result)
  end
end
