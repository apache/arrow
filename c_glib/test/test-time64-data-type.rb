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

class TestTime64DataType < Test::Unit::TestCase
  def test_type
    data_type = Arrow::Time64DataType.new(:micro)
    assert_equal(Arrow::Type::TIME64, data_type.id)
  end

  def test_invalid_unit
    message =
      "[time64-data-type][new] time unit must be micro or nano: <second>"
    assert_raise(Arrow::Error::Invalid.new(message)) do
      Arrow::Time64DataType.new(:second)
    end
  end

  sub_test_case("micro") do
    def setup
      @data_type = Arrow::Time64DataType.new(:micro)
    end

    def test_to_s
      assert_equal("time64[us]", @data_type.to_s)
    end
  end

  sub_test_case("nano") do
    def setup
      @data_type = Arrow::Time64DataType.new(:nano)
    end

    def test_to_s
      assert_equal("time64[ns]", @data_type.to_s)
    end
  end
end
