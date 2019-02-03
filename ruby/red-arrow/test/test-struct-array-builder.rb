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

class StructArrayBuilderTest < Test::Unit::TestCase
  def setup
    @data_type = Arrow::StructDataType.new(visible: {type: :boolean},
                                           count: {type: :uint64})
    @builder = Arrow::StructArrayBuilder.new(@data_type)
  end

  sub_test_case("#append_value") do
    test("nil") do
      @builder.append_value(nil)
      array = @builder.finish
      assert_equal([
                     [nil],
                     [nil],
                   ],
                   [
                     array.find_field(0).to_a,
                     array.find_field(1).to_a,
                   ])
    end

    test("Array") do
      @builder.append_value([true, 1])
      array = @builder.finish
      assert_equal([
                     [true],
                     [1],
                   ],
                   [
                     array.find_field(0).to_a,
                     array.find_field(1).to_a,
                   ])
    end

    test("Arrow::Struct") do
      source_array = Arrow::StructArray.new(@data_type, [[true, 1]])
      struct = source_array.get_value(0)
      @builder.append_value(struct)
      array = @builder.finish
      assert_equal([
                     [true],
                     [1],
                   ],
                   [
                     array.find_field(0).to_a,
                     array.find_field(1).to_a,
                   ])
    end

    test("Hash") do
      @builder.append_value(count: 1, visible: true)
      array = @builder.finish
      assert_equal([
                     [true],
                     [1],
                   ],
                   [
                     array.find_field(0).to_a,
                     array.find_field(1).to_a,
                   ])
    end
  end

  sub_test_case("#append_values") do
    test("[nil]") do
      @builder.append_values([nil])
      array = @builder.finish
      assert_equal([
                     [nil],
                     [nil],
                   ],
                   [
                     array.find_field(0).to_a,
                     array.find_field(1).to_a,
                   ])
    end

    test("[Array]") do
      @builder.append_values([[true, 1]])
      array = @builder.finish
      assert_equal([
                     [true],
                     [1],
                   ],
                   [
                     array.find_field(0).to_a,
                     array.find_field(1).to_a,
                   ])
    end

    test("[Hash]") do
      @builder.append_values([{count: 1, visible: true}])
      array = @builder.finish
      assert_equal([
                     [true],
                     [1],
                   ],
                   [
                     array.find_field(0).to_a,
                     array.find_field(1).to_a,
                   ])
    end

    test("[nil, Array, Hash]") do
      @builder.append_values([
                               nil,
                               [true, 1],
                               {count: 2, visible: false},
                             ])
      array = @builder.finish
      assert_equal([
                     [nil, true, false],
                     [nil, 1, 2],
                   ],
                   [
                     array.find_field(0).to_a,
                     array.find_field(1).to_a,
                   ])
    end

    test("is_valids") do
      @builder.append_values([
                               [true, 1],
                               [false, 2],
                               [true, 3],
                             ],
                             [
                               true,
                               false,
                               true,
                             ])
      array = @builder.finish
      assert_equal([
                     [true, nil, true],
                     [1, nil, 3],
                   ],
                   [
                     array.find_field(0).to_a,
                     array.find_field(1).to_a,
                   ])
    end
  end

  sub_test_case("#append") do
    test("backward compatibility") do
      @builder.append
      @builder.get_field_builder(0).append(true)
      @builder.get_field_builder(1).append(1)
      @builder.append
      @builder.get_field_builder(0).append(false)
      @builder.get_field_builder(1).append(2)
      array = @builder.finish
      assert_equal([
                     [true, 1],
                     [false, 2],
                   ],
                   [
                     array.get_value(0).values,
                     array.get_value(1).values,
                   ])
    end
  end
end
