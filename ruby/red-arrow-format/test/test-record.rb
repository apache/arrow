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

class TestRecord < Test::Unit::TestCase
  def setup
    @boolean_array = ArrowFormat::BooleanArray.new([true, nil, false])
    @int32_array = ArrowFormat::Int32Array.new([-(2 ** 31), 0, (2 ** 31) - 1])
    @record_batch = ArrowFormat::RecordBatch.new({
                                                   boolean: @boolean_array,
                                                   int32: @int32_array,
                                                 })
    @record = @record_batch.records[1]
  end

  sub_test_case("#[]") do
    test("Integer") do
      assert_equal(0, @record[1])
    end

    test("String") do
      assert_equal(0, @record["int32"])
    end

    test("Symbol") do
      assert_equal(0, @record[:int32])
    end
  end

  def test_to_a
    assert_equal([nil, 0], @record.to_a)
  end

  def test_to_h
    assert_equal({"boolean" => nil, "int32" => 0}, @record.to_h)
  end

  sub_test_case("#respond_to_missing?") do
    def test_existent
      assert do
        @record.respond_to?("int32")
      end
    end

    def test_nonexistent
      assert do
        not @record.respond_to?("nonexistent")
      end
    end
  end

  sub_test_case("#method_missing") do
    def test_existent
      assert_equal(0, @record.int32)
    end

    def test_existent_have_argument
      assert_raise(NoMethodError) do
        @record.int32(nil)
      end
    end

    def test_nonexitent
      assert_raise(NoMethodError) do
        @record.nonexistent
      end
    end
  end
end
