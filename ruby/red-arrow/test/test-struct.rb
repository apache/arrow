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

class StructTest < Test::Unit::TestCase
  def setup
    @data_type = Arrow::StructDataType.new(visible: {type: :boolean},
                                           count: {type: :uint64})
    @values = [
      [true, 1],
      [false, 2],
    ]
    @array = Arrow::StructArray.new(@data_type, @values)
    @struct = @array.get_value(0)
  end

  sub_test_case("#[]") do
    test("Integer") do
      assert_equal(true, @struct[0])
    end

    test("String") do
      assert_equal(true, @struct["visible"])
    end

    test("Symbol") do
      assert_equal(true, @struct[:visible])
    end
  end

  test("#fields") do
    assert_equal(@data_type.fields,
                 @struct.fields)
  end

  test("#values") do
    assert_equal([true, 1],
                 @struct.values)
  end

  test("#to_a") do
    assert_equal([true, 1],
                 @struct.to_a)
  end

  test("#to_h") do
    assert_equal({
                   "visible" => true,
                   "count" => 1,
                 },
                 @struct.to_h)
  end

  test("#respond_to_missing?") do
    assert_equal([
                   true,
                   false,
                 ],
                 [
                   @struct.respond_to?(:visible),
                   @struct.respond_to?(:nonexistent),
                 ])
  end

  test("#method_missing?") do
    assert_equal(1, @struct.count)
  end
end
