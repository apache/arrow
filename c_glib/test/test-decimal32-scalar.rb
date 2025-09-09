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

class TestDecimal32Scalar < Test::Unit::TestCase
  def setup
    @data_type = Arrow::Decimal32DataType.new(8, 2)
    @value = Arrow::Decimal32.new("23423445")
    @scalar = Arrow::Decimal32Scalar.new(@data_type, @value)
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
    assert_equal(Arrow::Decimal32Scalar.new(@data_type, @value),
                 @scalar)
  end

  def test_to_s
    assert_equal("234234.45", @scalar.to_s)
  end

  def test_value
    assert_equal(@value, @scalar.value)
  end
end
