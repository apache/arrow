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

class TestRoundToMultipleOptions < Test::Unit::TestCase
  include Helper::Buildable

  def setup
    @options = Arrow::RoundToMultipleOptions.new
  end

  def test_multiple
    assert_equal(Arrow::DoubleScalar.new(1.0),
                 @options.multiple)
    data_type = Arrow::Decimal256DataType.new(8, 2)
    value = Arrow::Decimal256.new("23423445")
    multiple = Arrow::Decimal256Scalar.new(data_type, value)
    @options.multiple = multiple
    assert_equal(multiple, @options.multiple)
  end

  def test_mode
    assert_equal(Arrow::RoundMode::HALF_TO_EVEN, @options.mode)
    @options.mode = :down
    assert_equal(Arrow::RoundMode::DOWN, @options.mode)
  end
end
