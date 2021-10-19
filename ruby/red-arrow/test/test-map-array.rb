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

class MapArrayTest < Test::Unit::TestCase
  sub_test_case(".new") do
    test("build") do
      key_type = Arrow::StringDataType.new
      item_type = Arrow::Int16DataType.new
      data_type = Arrow::MapDataType.new(key_type, item_type)
      values = [
        {"a" => 0, "b" => 1},
        nil,
        {"c" => 0, "d" => 1}
      ]
      array = Arrow::MapArray.new(data_type, values)
      assert_equal(values, array.collect {|value| value})
    end
  end
end
