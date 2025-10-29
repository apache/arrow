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

module RawRecordsMultipleColumnsTests
  def test_3_elements
    records = [
      [true, nil, "Ruby"],
      [nil, 0, "GLib"],
      [false, 2 ** 8 - 1, nil],
    ]
    target = build([
                     {name: :column0, type: :boolean},
                     {name: :column1, type: :uint8},
                     {name: :column2, type: :string},
                   ],
                   records)
    assert_equal(records, actual_records(target))
  end

  def test_4_elements
    records = [
      [true, nil, "Ruby", -(2 ** 63)],
      [nil, 0, "GLib", nil],
      [false, 2 ** 8 - 1, nil, (2 ** 63) - 1],
    ]
    target = build([
                     {name: :column0, type: :boolean},
                     {name: :column1, type: :uint8},
                     {name: :column2, type: :string},
                     {name: :column3, type: :int64},
                   ],
                   records)
    assert_equal(records, actual_records(target))
  end
end

class EachRawRecordRecordBatchMultipleColumnsTest < Test::Unit::TestCase
  include RawRecordsMultipleColumnsTests

  def build(schema, records)
    Arrow::RecordBatch.new(schema, records)
  end

  def actual_records(target)
    target.each_raw_record.to_a
  end
end

class EachRawRecordTableMultipleColumnsTest < Test::Unit::TestCase
  include RawRecordsMultipleColumnsTests

  def build(schema, records)
    record_batch = Arrow::RecordBatch.new(schema, records)
    # Multiple chunks
    record_batches = [
      record_batch.slice(0, 2),
      record_batch.slice(2, 0), # Empty chunk
      record_batch.slice(2, record_batch.length - 2),
    ]

    Arrow::Table.new(schema, record_batches)
  end

  def actual_records(target)
    target.each_raw_record.to_a
  end
end

class RawRecordsRecordBatchMultipleColumnsTest < Test::Unit::TestCase
  include RawRecordsMultipleColumnsTests

  def build(schema, records)
    Arrow::RecordBatch.new(schema, records)
  end

  def actual_records(target)
    target.raw_records
  end
end

class RawRecordsTableMultipleColumnsTest < Test::Unit::TestCase
  include RawRecordsMultipleColumnsTests

  def build(schema, records)
    Arrow::Table.new(schema, records)
  end

  def actual_records(target)
    target.raw_records
  end
end
