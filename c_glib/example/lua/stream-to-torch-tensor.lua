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

local torch = require 'torch'

Arrow.Array.torch_types = function(self)
   return nil
end

Arrow.Array.to_torch = function(self)
   local types = self:torch_types()
   if not types then
      return nil
   end

   local storage_type = types[1]
   local tensor_type = types[2]

   local size = self:get_length()
   local storage = storage_type(size)
   if not storage then
      return nil
   end

   for i = 1, size do
      storage[i] = self:get_value(i - 1)
   end
   return tensor_type(storage)
end

Arrow.UInt8Array.torch_types = function(self)
   return {torch.ByteStorage, torch.ByteTensor}
end

Arrow.Int8Array.torch_types = function(self)
   return {torch.CharStorage, torch.CharTensor}
end

Arrow.Int16Array.torch_types = function(self)
   return {torch.ShortStorage, torch.ShortTensor}
end

Arrow.Int32Array.torch_types = function(self)
   return {torch.IntStorage, torch.IntTensor}
end

Arrow.Int64Array.torch_types = function(self)
   return {torch.LongStorage, torch.LongTensor}
end

Arrow.FloatArray.torch_types = function(self)
   return {torch.FloatStorage, torch.FloatTensor}
end

Arrow.DoubleArray.torch_types = function(self)
   return {torch.DoubleStorage, torch.DoubleTensor}
end


local input_path = arg[1] or "/tmp/stream.arrow";

local input = Arrow.MemoryMappedInputStream.new(input_path)
local reader = Arrow.RecordBatchStreamReader.new(input)

local i = 0
while true do
   local record_batch = reader:get_next_record_batch()
   if not record_batch then
      break
   end

   print(string.rep("=", 40))
   print("record-batch["..i.."]:")
   for j = 0, record_batch:get_n_columns() - 1 do
      local column = record_batch:get_column(j)
      local column_name = record_batch:get_column_name(j)
      print("  "..column_name..":")
      print(column:to_torch())
   end

   i = i + 1
end

input:close()
