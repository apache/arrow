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
  sub_test_case(".new") do
    def setup
      @schema = Arrow::Schema.new(visible: :boolean,
                                  count: :uint32)
    end

    test("[Schema, records]") do
      records = [
        {visible: true, count: 1},
        nil,
        [false, 3],
      ]
      record_batch = Arrow::RecordBatch.new(@schema, records)
      assert_equal([
                     {"visible" => true,  "count" => 1},
                     {"visible" => nil,   "count" => nil},
                     {"visible" => false, "count" => 3},
                   ],
                   record_batch.each_record.collect(&:to_h))
    end

    test("[Schema, columns]") do
      columns = {
        visible: [true, nil, false],
        count: [1, 2, nil],
      }
      record_batch = Arrow::RecordBatch.new(@schema, columns)
      assert_equal([
                     {"visible" => true,  "count" => 1},
                     {"visible" => nil,   "count" => 2},
                     {"visible" => false, "count" => nil},
                   ],
                   record_batch.each_record.collect(&:to_h))
    end

    test("[Schema, n_rows, columns]") do
      columns = [
        Arrow::BooleanArray.new([true, nil, false]),
        Arrow::UInt32Array.new([1, 2, nil]),
      ]
      n_rows = columns[0].length
      record_batch = Arrow::RecordBatch.new(@schema, n_rows, columns)
      assert_equal([
                     {"visible" => true,  "count" => 1},
                     {"visible" => nil,   "count" => 2},
                     {"visible" => false, "count" => nil},
                   ],
                   record_batch.each_record.collect(&:to_h))
    end
  end

  sub_test_case("instance methods") do
    def setup
      @schema = Arrow::Schema.new(count: :uint32)
      @counts = Arrow::UInt32Array.new([1, 2, 4, 8])
      @record_batch = Arrow::RecordBatch.new(@schema, @counts.length, [@counts])
    end

    sub_test_case("#each") do
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

    sub_test_case("#==") do
      test("Arrow::RecordBatch") do
        assert do
          @record_batch == @record_batch
        end
      end

      test("not Arrow::RecordBatch") do
        assert do
          not (@record_batch == 29)
        end
      end
    end
  end
end
