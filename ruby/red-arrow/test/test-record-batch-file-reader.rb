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

class RecordBatchFileReaderTest < Test::Unit::TestCase
  test("write/read") do
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

    tempfile = Tempfile.new(["batch", ".arrow"])
    Arrow::FileOutputStream.open(tempfile.path, false) do |output|
      Arrow::RecordBatchFileWriter.open(output, schema) do |writer|
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
      end
    end

    Arrow::MemoryMappedInputStream.open(tempfile.path) do |input|
      reader = Arrow::RecordBatchFileReader.new(input)
      reader.each do |record_batch|
        assert_equal([
                       {
                         "uint8"  => 1,
                         "uint16" => 1,
                         "uint32" => 1,
                         "uint64" => 1,
                         "int8"   => 1,
                         "int16"  => 1,
                         "int32"  => 1,
                         "int64"  => 1,
                         "float"  => 1.100000023841858,
                         "double" => 1.1,
                       },
                       {
                         "uint8"  => 2,
                         "uint16" => 2,
                         "uint32" => 2,
                         "uint64" => 2,
                         "int8"   => -2,
                         "int16"  => -2,
                         "int32"  => -2,
                         "int64"  => -2,
                         "float"  => -2.200000047683716,
                         "double" => -2.2,
                       },
                       {
                         "uint8"  => 4,
                         "uint16" => 4,
                         "uint32" => 4,
                         "uint64" => 4,
                         "int8"   => 4,
                         "int16"  => 4,
                         "int32"  => 4,
                         "int64"  => 4,
                         "float"  => 4.400000095367432,
                         "double" => 4.4,
                       },
                       {
                         "uint8"  => 8,
                         "uint16" => 8,
                         "uint32" => 8,
                         "uint64" => 8,
                         "int8"   => -8,
                         "int16"  => -8,
                         "int32"  => -8,
                         "int64"  => -8,
                         "float"  => -8.800000190734863,
                         "double" => -8.8,
                       },
                     ],
                     record_batch.collect(&:to_h))
      end
    end
  end

  sub_test_case("#each") do
    test("without block") do
      buffer = Arrow::ResizableBuffer.new(1024)
      Arrow::Table.new(number: [1, 2, 3]).save(buffer)
      Arrow::BufferInputStream.open(buffer) do |input|
        reader = Arrow::RecordBatchFileReader.new(input)
        each = reader.each
        assert_equal({
                       size: 1,
                       to_a: [
                         Arrow::RecordBatch.new(number: [1, 2, 3]),
                       ],
                     },
                     {
                       size: each.size,
                       to_a: each.to_a,
                     })
      end
    end
  end
end
