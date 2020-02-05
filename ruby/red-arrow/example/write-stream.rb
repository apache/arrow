#!/usr/bin/env ruby
#
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

require "arrow"

fields = [
  Arrow::Field.new("uint8",  :uint8),
  Arrow::Field.new("uint16", :uint16),
  Arrow::Field.new("uint32", :uint32),
  Arrow::Field.new("uint64", :uint64),
  Arrow::Field.new("int8",   :int8),
  Arrow::Field.new("int16",  :int16),
  Arrow::Field.new("int32",  :int32),
  Arrow::Field.new("int64",  :int64),
  Arrow::Field.new("float",  :float),
  Arrow::Field.new("double", :double),
]
schema = Arrow::Schema.new(fields)

Arrow::FileOutputStream.open("/tmp/stream.arrow", false) do |output|
  Arrow::RecordBatchStreamWriter.open(output, schema) do |writer|
    uints = [1, 2, 4, 8]
    ints = [1, -2, 4, -8]
    floats = [1.1, -2.2, 4.4, -8.8]
    columns = [
      Arrow::UInt8Array.new(uints),
      Arrow::UInt16Array.new(uints),
      Arrow::UInt32Array.new(uints),
      Arrow::UInt64Array.new(uints),
      Arrow::Int8Array.new(ints),
      Arrow::Int16Array.new(ints),
      Arrow::Int32Array.new(ints),
      Arrow::Int64Array.new(ints),
      Arrow::FloatArray.new(floats),
      Arrow::DoubleArray.new(floats),
    ]

    record_batch = Arrow::RecordBatch.new(schema, 4, columns)
    writer.write_record_batch(record_batch)

    sliced_columns = columns.collect do |column|
      column.slice(1, 3)
    end
    record_batch = Arrow::RecordBatch.new(schema, 3, sliced_columns)
    writer.write_record_batch(record_batch)
  end
end
