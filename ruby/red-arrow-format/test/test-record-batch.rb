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

class TestRecordBatch < Test::Unit::TestCase
  def setup
    @boolean_array = ArrowFormat::BooleanArray.new([true, nil, false])
    @int32_array = ArrowFormat::Int32Array.new([-(2 ** 31), 0, (2 ** 31) - 1])
    @record_batch = ArrowFormat::RecordBatch.new({
                                                   boolean: @boolean_array,
                                                   int32: @int32_array,
                                                 })
  end

  sub_test_case("#initialize") do
    test("{}") do
      error = ArgumentError.new("no data")
      assert_raise(error) do
        ArrowFormat::RecordBatch.new({})
      end
    end

    test("{Symbol => Array}") do
      raw_records = {
        boolean: @boolean_array,
        int32: @int32_array,
      }
      record_batch = ArrowFormat::RecordBatch.new(raw_records)
      assert_equal({
                     "boolean" => @boolean_array,
                     "int32" => @int32_array,
                   },
                   record_batch.to_h)
    end

    test("{String => Array}") do
      raw_records = {
        "boolean" => @boolean_array,
        "int32" => @int32_array,
      }
      record_batch = ArrowFormat::RecordBatch.new(raw_records)
      assert_equal(raw_records, record_batch.to_h)
    end

    test("[{}]") do
      raw_records = [
        {boolean: true,  int32: -(2 ** 31)},
        {                int32: 0},
        {boolean: false, int32: (2 ** 31) - 1},
      ]
      record_batch = ArrowFormat::RecordBatch.new(raw_records)
      assert_equal({
                     "boolean" => @boolean_array,
                     "int32" => @int32_array,
                   },
                   record_batch.to_h)
    end

    test("inconsistent n_rows") do
      raw_records = {
        boolean: ArrowFormat::BooleanArray.new([true, nil]),
        int32: ArrowFormat::Int32Array.new([-(2 ** 31), 0, (2 ** 31) - 1]),
      }
      error = ArgumentError.new("inconsistent the number of rows: 2, 3")
      assert_raise(error) do
        ArrowFormat::RecordBatch.new(raw_records)
      end
    end
  end

  sub_test_case("#find_column") do
    test("Integer") do
      assert_equal(@int32_array, @record_batch.find_column(1))
    end

    test("String") do
      assert_equal(@int32_array, @record_batch.find_column("int32"))
    end

    test("Symbol") do
      assert_equal(@int32_array, @record_batch.find_column(:int32))
    end
  end

  sub_test_case("#each_record") do
    test("default") do
      assert_equal([
                     [true, -(2 ** 31)],
                     [nil, 0],
                     [false, (2 ** 31) - 1],
                   ],
                   @record_batch.each_record.collect(&:to_a))
    end

    test(":reuse_record") do
      actual = []
      @record_batch.each_record(reuse_record: true) do |record|
        actual << [record.object_id, record.index]
      end
      assert_equal([
                     [actual[0][0], 0],
                     [actual[0][0], 1],
                     [actual[0][0], 2],
                   ],
                   actual)
    end
  end

  test("#records") do
    assert_equal([
                   {"boolean" => true,  "int32" => -(2 ** 31)},
                   {"boolean" => nil,   "int32" => 0},
                   {"boolean" => false, "int32" => (2 ** 31) - 1},
                 ],
                 @record_batch.records.collect(&:to_h))
  end
end
