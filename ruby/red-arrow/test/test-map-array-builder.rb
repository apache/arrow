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

class MapArrayBuilderTest < Test::Unit::TestCase
  def setup
    key_type = Arrow::StringDataType.new
    item_type = Arrow::Int16DataType.new
    data_type = Arrow::MapDataType.new(key_type, item_type)
    @builder = Arrow::MapArrayBuilder.new(data_type)
  end

  sub_test_case("#append_value") do
    test("nil") do
      @builder.append_value(nil)
      array = @builder.finish
      assert_equal([nil], array.collect {|value| value})
    end

    test("Hash") do
      @builder.append_value({"a" => 0, "b" => 1})
      @builder.append_value({"c" => 0, "d" => 1})
      array = @builder.finish
      assert_equal([
                     {"a" => 0, "b" => 1},
                     {"c" => 0, "d" => 1}
                   ],
                   array.collect {|value| value})
    end

    test("#each") do
      @builder.append_value([["a", 0], ["b", 1]])
      @builder.append_value([["c", 0], ["d", 1]])
      array = @builder.finish
      assert_equal([
                     {"a" => 0, "b" => 1},
                     {"c" => 0, "d" => 1}
                   ],
                   array.collect {|value| value})
    end
  end

  sub_test_case("#append_values") do
    test("[nil]") do
      @builder.append_values([nil])
      array = @builder.finish
      assert_equal([nil], array.collect {|value| value})
    end

    test("[Hash]") do
      @builder.append_values([{"a" => 0, "b" => 1}, {"c" => 0, "d" => 1}])
      array = @builder.finish
      assert_equal([
                     {"a" => 0, "b" => 1},
                     {"c" => 0, "d" => 1}
                   ],
                   array.collect {|value| value})
    end

    test("[#each]") do
      @builder.append_values([[["a", 0], ["b", 1]], [["c", 0], ["d", 1]]])
      array = @builder.finish
      assert_equal([
                     {"a" => 0, "b" => 1},
                     {"c" => 0, "d" => 1}
                   ],
                   array.collect {|value| value})
    end

    test("[nil, Hash, #each]") do
      @builder.append_values([nil, {"a" => 0, "b" => 1}, [["c", 0], ["d", 1]]])
      array = @builder.finish
      assert_equal([
                     nil,
                     {"a" => 0, "b" => 1},
                     {"c" => 0, "d" => 1}
                   ],
                   array.collect {|value| value})
    end

    test("is_valids") do
      @builder.append_values([
                               {"a" => 0, "b" => 1},
                               {"c" => 0, "d" => 1},
                               {"e" => 0, "f" => 1}
                             ],
                             [true, false, true])
      array = @builder.finish
      assert_equal([
                     {"a" => 0, "b" => 1},
                     nil,
                     {"e" => 0, "f" => 1}
                   ],
                   array.collect {|value| value})
    end
  end
end
