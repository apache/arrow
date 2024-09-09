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

class TestArrowFileWriter < Test::Unit::TestCase
  def open_buffer_output_stream
    buffer = Arrow::ResizableBuffer.new(4096)
    Arrow::BufferOutputStream.open(buffer) do |output|
      yield(output)
    end
    buffer
  end

  sub_test_case("#write") do
    test("RecordBatch") do
      schema = Arrow::Schema.new(visible: :boolean)
      record_batch = Arrow::RecordBatch.new(schema, [[true], [false]])
      buffer = open_buffer_output_stream do |output|
        Parquet::ArrowFileWriter.open(record_batch.schema, output) do |writer|
          writer.write(record_batch)
        end
      end
      assert_equal(record_batch.to_table,
                   Arrow::Table.load(buffer, format: :parquet))
    end

    test("Table") do
      schema = Arrow::Schema.new(visible: :boolean)
      table = Arrow::Table.new(schema, [[true], [false]])
      buffer = open_buffer_output_stream do |output|
        Parquet::ArrowFileWriter.open(table.schema, output) do |writer|
          writer.write(table)
        end
      end
      assert_equal(table,
                   Arrow::Table.load(buffer, format: :parquet))
    end

    test("[[]]") do
      schema = Arrow::Schema.new(visible: :boolean)
      raw_records = [[true], [false]]
      buffer = open_buffer_output_stream do |output|
        Parquet::ArrowFileWriter.open(schema, output) do |writer|
          writer.write(raw_records)
        end
      end
      assert_equal(Arrow::RecordBatch.new(schema, raw_records).to_table,
                   Arrow::Table.load(buffer, format: :parquet))
    end

    test("[{}]") do
      schema = Arrow::Schema.new(visible: :boolean)
      raw_columns = [visible: [true, false]]
      buffer = open_buffer_output_stream do |output|
        Parquet::ArrowFileWriter.open(schema, output) do |writer|
          writer.write(raw_columns)
        end
      end
      assert_equal(Arrow::RecordBatch.new(schema, raw_columns).to_table,
                   Arrow::Table.load(buffer, format: :parquet))
    end
  end
end
