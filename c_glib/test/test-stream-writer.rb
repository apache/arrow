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

class TestStreamWriter < Test::Unit::TestCase
  include Helper::Buildable

  def test_write_record_batch
    data = [true]
    field = Arrow::Field.new("enabled", Arrow::BooleanDataType.new)
    schema = Arrow::Schema.new([field])

    tempfile = Tempfile.open("arrow-ipc-stream-writer")
    output = Arrow::FileOutputStream.new(tempfile.path, false)
    begin
      stream_writer = Arrow::RecordBatchStreamWriter.new(output, schema)
      begin
        columns = [
          build_boolean_array(data),
        ]
        record_batch = Arrow::RecordBatch.new(schema, data.size, columns)
        stream_writer.write_record_batch(record_batch)
      ensure
        stream_writer.close
      end
    ensure
      output.close
    end

    input = Arrow::MemoryMappedInputStream.new(tempfile.path)
    begin
      stream_reader = Arrow::RecordBatchStreamReader.new(input)
      assert_equal([field.name],
                   stream_reader.schema.fields.collect(&:name))
      assert_equal(Arrow::RecordBatch.new(schema,
                                          data.size,
                                          [build_boolean_array(data)]),
                   stream_reader.read_next)
      assert_nil(stream_reader.read_next)
    ensure
      input.close
    end
  end
end
