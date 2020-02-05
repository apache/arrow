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

class Decimal128DataTypeTest < Test::Unit::TestCase
  sub_test_case(".new") do
    test("ordered arguments") do
      assert_equal("decimal(8, 2)",
                   Arrow::Decimal128DataType.new(8, 2).to_s)
    end

    test("description") do
      assert_equal("decimal(8, 2)",
                   Arrow::Decimal128DataType.new(precision: 8,
                                                 scale: 2).to_s)
    end
  end
end
