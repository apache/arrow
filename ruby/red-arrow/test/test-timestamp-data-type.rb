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

class TimestampDataTypeTest < Test::Unit::TestCase
  sub_test_case(".new") do
    test("Arrow::TimeUnit") do
      assert_equal("timestamp[ms]",
                   Arrow::TimestampDataType.new(Arrow::TimeUnit::MILLI).to_s)
    end

    test("Symbol") do
      assert_equal("timestamp[ms]",
                   Arrow::TimestampDataType.new(:milli).to_s)
    end

    test("unit: Arrow::TimeUnit") do
      data_type = Arrow::TimestampDataType.new(unit: Arrow::TimeUnit::MILLI)
      assert_equal("timestamp[ms]",
                   data_type.to_s)
    end

    test("unit: Symbol") do
      data_type = Arrow::TimestampDataType.new(unit: :milli)
      assert_equal("timestamp[ms]",
                   data_type.to_s)
    end
  end
end
