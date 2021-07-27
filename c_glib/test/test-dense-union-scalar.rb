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

class TestDenseUnionScalar < Test::Unit::TestCase
  def setup
    fields = [
      Arrow::Field.new("number", Arrow::Int8DataType.new),
      Arrow::Field.new("text", Arrow::StringDataType.new),
    ]
    @data_type = Arrow::DenseUnionDataType.new(fields, [2, 9])
    @type_code = 2
    @value = Arrow::Int8Scalar.new(-29)
    @scalar = Arrow::DenseUnionScalar.new(@data_type, @type_code, @value)
  end

  def test_type_code
    assert_equal(@type_code,
                 @scalar.type_code)
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
    assert_equal(Arrow::DenseUnionScalar.new(@data_type, @type_code, @value),
                 @scalar)
  end

  def test_to_s
    assert_equal("...", @scalar.to_s)
  end

  def test_value
    assert_equal(@value, @scalar.value)
  end
end
