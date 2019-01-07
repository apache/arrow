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

class RecordBatchTest < Test::Unit::TestCase
  setup do
    fields = [
      Arrow::Field.new("count", :uint32),
    ]
    @schema = Arrow::Schema.new(fields)
    @counts = Arrow::UInt32Array.new([1, 2, 4, 8])
    @record_batch = Arrow::RecordBatch.new(@schema, @counts.length, [@counts])
  end

  sub_test_case(".each") do
    test("default") do
      records = []
      @record_batch.each do |record|
        records << [record, record.index]
      end
      assert_equal([
                     [0, 0],
                     [1, 1],
                     [2, 2],
                     [3, 3],
                   ],
                   records.collect {|record, i| [record.index, i]})
    end

    test("reuse_record: true") do
      records = []
      @record_batch.each(reuse_record: true) do |record|
        records << [record, record.index]
      end
      assert_equal([
                     [3, 0],
                     [3, 1],
                     [3, 2],
                     [3, 3],
                   ],
                   records.collect {|record, i| [record.index, i]})
    end
  end

  test("#to_table") do
    assert_equal(Arrow::Table.new(@schema, [@counts]),
                 @record_batch.to_table)
  end
end
