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

class BigDecimalTest < Test::Unit::TestCase
  sub_test_case("#to_arrow") do
    def test_128_positive
      assert_equal(Arrow::Decimal128.new("0.1e38"),
                   BigDecimal("0.1e38").to_arrow)
    end

    def test_128_negative
      assert_equal(Arrow::Decimal128.new("-0.1e38"),
                   BigDecimal("-0.1e38").to_arrow)
    end

    def test_256_positive
      assert_equal(Arrow::Decimal256.new("0.1e39"),
                   BigDecimal("0.1e39").to_arrow)
    end

    def test_256_negative
      assert_equal(Arrow::Decimal256.new("-0.1e39"),
                   BigDecimal("-0.1e39").to_arrow)
    end
  end
end
