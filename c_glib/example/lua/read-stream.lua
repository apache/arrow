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

local input_path = arg[1] or "/tmp/stream.arrow";

local input = Arrow.MemoryMappedInputStream.new(input_path)
local reader = Arrow.StreamReader.open(input)

local i = 0
while true do
   local record_batch = reader:get_next_record_batch(i)
   if not record_batch then
      break
   end

   print(string.rep("=", 40))
   print("record-batch["..i.."]:")
   for j = 0, record_batch:get_n_columns() - 1 do
      local column = record_batch:get_column(j)
      local column_name = record_batch:get_column_name(j)
      io.write("  "..column_name..": [")
      for k = 0, record_batch:get_n_rows() - 1 do
	 if k > 0 then
	    io.write(", ")
	 end
	 io.write(column:get_value(k))
      end
      print("]")
   end

   i = i + 1
end

input:close()
