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
  sub_test_case("#write") do
    test("RecordBatch") do
      schema = Arrow::Schema.new(visible: :boolean)
      record_batch = Arrow::RecordBatch.new(schema, [[true], [false]])
      Tempfile.create(["red-parquet", ".parquet"]) do |file|
        Parquet::ArrowFileWriter.open(record_batch.schema, file.path) do |writer|
          writer.write(record_batch)
        end
        assert_equal(record_batch.to_table,
                     Arrow::Table.load(file.path))
      end
    end

    test("Table") do
      schema = Arrow::Schema.new(visible: :boolean)
      table = Arrow::Table.new(schema, [[true], [false]])
      Tempfile.create(["red-parquet", ".parquet"]) do |file|
        Parquet::ArrowFileWriter.open(table.schema, file.path) do |writer|
          writer.write(table)
        end
        assert_equal(table,
                     Arrow::Table.load(file.path))
      end
    end

    test("[[]]") do
      schema = Arrow::Schema.new(visible: :boolean)
      raw_records = [[true], [false]]
      Tempfile.create(["red-parquet", ".parquet"]) do |file|
        Parquet::ArrowFileWriter.open(schema, file.path) do |writer|
          writer.write(raw_records)
        end
        assert_equal(Arrow::RecordBatch.new(schema, raw_records).to_table,
                     Arrow::Table.load(file.path))
      end
    end

    test("[{}]") do
      schema = Arrow::Schema.new(visible: :boolean)
      raw_columns = [visible: [true, false]]
      Tempfile.create(["red-parquet", ".parquet"]) do |file|
        Parquet::ArrowFileWriter.open(schema, file.path) do |writer|
          writer.write(raw_columns)
        end
        assert_equal(Arrow::RecordBatch.new(schema, raw_columns).to_table,
                     Arrow::Table.load(file.path))
      end
    end
  end
end
