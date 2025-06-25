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

module Parquet
  class ArrowFileWriter
    # Write data to Apache Parquet.
    #
    # @return [void]
    #
    # @overload write(record_batch)
    #
    #   @param record_batch [Arrow::RecordBatch] The record batch to
    #     be written.
    #
    #   @example Write a record batch
    #     record_batch = Arrow::RecordBatch.new(enabled: [true, false])
    #     schema = record_batch.schema
    #     Parquet::ArrowFileWriter.open(schema, "data.parquet") do |writer|
    #       writer.write(record_batch)
    #     end
    #
    # @overload write(table, chunk_size: nil)
    #
    #   @param table [Arrow::Table] The table to be written.
    #
    #   @param chunk_size [nil, Integer] (nil) The maximum number of
    #     rows to write per row group.
    #
    #     If this is `nil`, the default value (`1024 * 1024`) is used.
    #
    #   @example Write a record batch with the default chunk size
    #     table = Arrow::Table.new(enabled: [true, false])
    #     schema = table.schema
    #     Parquet::ArrowFileWriter.open(schema, "data.parquet") do |writer|
    #       writer.write(table)
    #     end
    #
    #   @example Write a record batch with the specified chunk size
    #     table = Arrow::Table.new(enabled: [true, false])
    #     schema = table.schema
    #     Parquet::ArrowFileWriter.open(schema, "data.parquet") do |writer|
    #       writer.write(table, chunk_size: 1)
    #     end
    #
    # @overload write(raw_records)
    #
    #   @param data [Array<Hash>, Array<Array>] The data to be written
    #     as primitive Ruby objects.
    #
    #   @example Write a record batch with Array<Array> based data
    #     schema = Arrow::Schema.new(enabled: :boolean)
    #     raw_records = [
    #       [true],
    #       [false],
    #     ]
    #     Parquet::ArrowFileWriter.open(schema, "data.parquet") do |writer|
    #       writer.write(raw_records)
    #     end
    #
    #   @example Write a record batch with Array<Hash> based data
    #     schema = Arrow::Schema.new(enabled: :boolean)
    #     raw_columns = [
    #       enabled: [true, false],
    #     ]
    #     Parquet::ArrowFileWriter.open(schema, "data.parquet") do |writer|
    #       writer.write(raw_columns)
    #     end
    #
    # @since 18.0.0
    def write(target, chunk_size: nil)
      case target
      when Arrow::RecordBatch
        write_record_batch(target)
      when Arrow::Table
        # Same as parquet::DEFAULT_MAX_ROW_GROUP_LENGTH in C++
        chunk_size ||= 1024 * 1024
        write_table(target, chunk_size)
      else
        record_batch = Arrow::RecordBatch.new(schema, target)
        write_record_batch(record_batch)
      end
    end
  end
end
