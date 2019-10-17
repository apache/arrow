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

class TestExpressionBuilderAdd < Test::Unit::TestCase
  def setup
    @table = Arrow::Table.new(int32_field: Arrow::Int32Array.new([1]),
                              int64_field: Arrow::Int64Array.new([2]))
    @schema = @table.schema
  end

  def build
    record = Gandiva::ExpressionBuilder::Record.new(@schema)
    builder = yield(record)
    builder.build
  end

  test("literal") do
    node = build do |record|
      record.int32_field + (2 ** 63)
    end
    assert_equal("uint64 add((int32) int32_field, (const uint64) #{2 ** 63})",
                 node.to_s)
  end

  test("int32 + int64") do
    node = build do |record|
      record.int32_field + record.int64_field
    end
    assert_equal("int64 add((int32) int32_field, (int64) int64_field)",
                 node.to_s)
  end

  test("int64 + int32") do
    node = build do |record|
      record.int64_field + record.int32_field
    end
    assert_equal("int64 add((int64) int64_field, (int32) int32_field)",
                 node.to_s)
  end
end
