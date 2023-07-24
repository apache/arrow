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

class TestHalfFloatScalar < Test::Unit::TestCase
  def setup
    @half_float = 0x3c01 # 1.0009765625
    @scalar = Arrow::HalfFloatScalar.new(@half_float)
  end

  def test_data_type
    assert_equal(Arrow::HalfFloatDataType.new,
                 @scalar.data_type)
  end

  def test_valid?
    assert do
      @scalar.valid?
    end
  end

  def test_equal
    options = Arrow::EqualOptions.new
    options.approx = true
    assert do
      @scalar.equal_options(Arrow::HalfFloatScalar.new(@half_float), options)
    end
  end

  def test_to_s
    assert_equal("[\n  #{@half_float}\n]", @scalar.to_s)
  end

  def test_value
    assert_in_delta(@half_float, @scalar.value)
  end
end
