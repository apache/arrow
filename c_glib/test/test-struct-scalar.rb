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

class TestStructScalar < Test::Unit::TestCase
  include Helper::Buildable

  def setup
    fields = [
      Arrow::Field.new("score", Arrow::Int8DataType.new),
      Arrow::Field.new("enabled", Arrow::BooleanDataType.new),
    ]
    @data_type = Arrow::StructDataType.new(fields)
    @value = [
      Arrow::Int8Scalar.new(-29),
      Arrow::BooleanScalar.new(true),
    ]
    @scalar = Arrow::StructScalar.new(@data_type, @value)
  end

  def test_data_type
    assert_equal(@data_type,
                 @scalar.data_type)
  end

  def test_valid?
    assert do
      @scalar.valid?
    end
  end

  def test_equal
    assert_equal(Arrow::StructScalar.new(@data_type, @value),
                 @scalar)
  end

  def test_to_s
    assert_equal("{score:int8 = -29, enabled:bool = true}", @scalar.to_s)
  end

  def test_value
    assert_equal(@value, @scalar.value)
  end

  def test_from_cpp
    min_max = Arrow::Function.find("min_max")
    args = [
      Arrow::ArrayDatum.new(build_int8_array([0, 2, -4])),
    ]
    scalar = min_max.execute(args).value
    assert_equal([
                   Arrow::Int8Scalar.new(-4),
                   Arrow::Int8Scalar.new(2),
                 ],
                 scalar.value)
  end
end
