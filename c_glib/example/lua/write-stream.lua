-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.

local lgi = require 'lgi'
local Arrow = lgi.Arrow

local output_path = arg[1] or "/tmp/stream.arrow";

local fields = {
  Arrow.Field.new("uint8",  Arrow.UInt8DataType.new()),
  Arrow.Field.new("uint16", Arrow.UInt16DataType.new()),
  Arrow.Field.new("uint32", Arrow.UInt32DataType.new()),
  Arrow.Field.new("uint64", Arrow.UInt64DataType.new()),
  Arrow.Field.new("int8",   Arrow.Int8DataType.new()),
  Arrow.Field.new("int16",  Arrow.Int16DataType.new()),
  Arrow.Field.new("int32",  Arrow.Int32DataType.new()),
  Arrow.Field.new("int64",  Arrow.Int64DataType.new()),
  Arrow.Field.new("float",  Arrow.FloatDataType.new()),
  Arrow.Field.new("double", Arrow.DoubleDataType.new()),
}
local schema = Arrow.Schema.new(fields)

local output = Arrow.FileOutputStream.new(output_path, false)
local writer = Arrow.RecordBatchStreamWriter.new(output, schema)

function build_array(builder, values)
   for _, value in pairs(values) do
      builder:append(value)
   end
   return builder:finish()
end

local uints = {1, 2, 4, 8}
local ints = {1, -2, 4, -8}
local floats = {1.1, -2.2, 4.4, -8.8}
local columns = {
   build_array(Arrow.UInt8ArrayBuilder.new(), uints),
   build_array(Arrow.UInt16ArrayBuilder.new(), uints),
   build_array(Arrow.UInt32ArrayBuilder.new(), uints),
   build_array(Arrow.UInt64ArrayBuilder.new(), uints),
   build_array(Arrow.Int8ArrayBuilder.new(), ints),
   build_array(Arrow.Int16ArrayBuilder.new(), ints),
   build_array(Arrow.Int32ArrayBuilder.new(), ints),
   build_array(Arrow.Int64ArrayBuilder.new(), ints),
   build_array(Arrow.FloatArrayBuilder.new(), floats),
   build_array(Arrow.DoubleArrayBuilder.new(), floats),
}

local record_batch = Arrow.RecordBatch.new(schema, 4, columns)
writer:write_record_batch(record_batch)

local sliced_columns = {}
for i, column in pairs(columns) do
   sliced_columns[i] = column:slice(1, 3)
end
record_batch = Arrow.RecordBatch.new(schema, 3, sliced_columns)
writer:write_record_batch(record_batch)

writer:close()
output:close()
