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

class Time64DataTypeTest < Test::Unit::TestCase
  sub_test_case(".new") do
    test("Arrow::TimeUnit") do
      assert_equal("time64[ns]",
                   Arrow::Time64DataType.new(Arrow::TimeUnit::NANO).to_s)
    end

    test("Symbol") do
      assert_equal("time64[ns]",
                   Arrow::Time64DataType.new(:nano).to_s)
    end

    test("unit: Arrow::TimeUnit") do
      data_type = Arrow::Time64DataType.new(unit: Arrow::TimeUnit::NANO)
      assert_equal("time64[ns]",
                   data_type.to_s)
    end

    test("unit: Symbol") do
      data_type = Arrow::Time64DataType.new(unit: :nano)
      assert_equal("time64[ns]",
                   data_type.to_s)
    end
  end
end
