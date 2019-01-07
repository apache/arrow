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

class ListArrayBuilderTest < Test::Unit::TestCase
  def setup
    @data_type = Arrow::ListDataType.new(name: "visible", type: :boolean)
    @builder = Arrow::ListArrayBuilder.new(@data_type)
  end

  sub_test_case("#append_value") do
    test("nil") do
      @builder.append_value(nil)
      array = @builder.finish
      assert_equal(nil, array[0])
    end

    test("Array") do
      @builder.append_value([true, false, true])
      array = @builder.finish
      assert_equal([true, false, true], array[0].to_a)
    end
  end

  sub_test_case("#append_values") do
    test("[nil, Array]") do
      @builder.append_values([[false], nil, [true, false, true]])
      array = @builder.finish
      assert_equal([
                     [false],
                     nil,
                     [true, false, true],
                   ],
                   array.collect {|list| list ? list.to_a : nil})
    end

    test("is_valids") do
      @builder.append_values([[false], [true, true], [true, false, true]],
                             [true, false, true])
      array = @builder.finish
      assert_equal([
                     [false],
                     nil,
                     [true, false, true],
                   ],
                   array.collect {|list| list ? list.to_a : nil})
    end
  end
end
