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

class TestDatasetFileWriter < Test::Unit::TestCase
  include Helper::Buildable
  include Helper::Readable

  def setup
    omit("Arrow Dataset is required") unless defined?(ArrowDataset)
    Dir.mktmpdir do |tmpdir|
      @dir = tmpdir
      @format = ArrowDataset::IPCFileFormat.new
      @file_system = Arrow::LocalFileSystem.new
      @path = File.join(@dir, "data.arrow")
      @output = @file_system.open_output_stream(@path)
      @schema = build_schema(visible: Arrow::BooleanDataType.new,
                             point: Arrow::UInt8DataType.new)
      @writer = @format.open_writer(@output,
                                    @file_system,
                                    @path,
                                    @schema,
                                    @format.default_write_options)
      yield
    end
  end

  def test_write_record_batch
    record_batch = build_record_batch(
      visible: build_boolean_array([true, false, true]),
      point: build_uint8_array([1, 2, 3]))
    @writer.write_record_batch(record_batch)
    @writer.finish
    @output.close
    read_table(@path) do |written_table|
      assert_equal(Arrow::Table.new(record_batch.schema,
                                    [record_batch]),
                   written_table)
    end
  end

  def test_write_record_batch_reader
    table = build_table(visible: build_boolean_array([true, false, true]),
                        point: build_uint8_array([1, 2, 3]))
    @writer.write_record_batch_reader(Arrow::TableBatchReader.new(table))
    @writer.finish
    @output.close
    read_table(@path) do |written_table|
      assert_equal(table, written_table)
    end
  end
end
