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

class TestTime32DataType < Test::Unit::TestCase
  def test_type
    data_type = Arrow::Time32DataType.new(:second)
    assert_equal(Arrow::Type::TIME32, data_type.id)
  end

  def test_invalid_unit
    message =
      "[time32-data-type][new] time unit must be second or milli: <micro>"
    assert_raise(Arrow::Error::Invalid.new(message)) do
      Arrow::Time32DataType.new(:micro)
    end
  end

  sub_test_case("second") do
    def setup
      @data_type = Arrow::Time32DataType.new(:second)
    end

    def test_to_s
      assert_equal("time32[s]", @data_type.to_s)
    end
  end

  sub_test_case("milli") do
    def setup
      @data_type = Arrow::Time32DataType.new(:milli)
    end

    def test_to_s
      assert_equal("time32[ms]", @data_type.to_s)
    end
  end
end
