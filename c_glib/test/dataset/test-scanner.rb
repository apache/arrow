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

class TestDatasetScanner < Test::Unit::TestCase
  include Helper::Buildable
  include Helper::Writable

  def setup
    omit("Arrow Dataset is required") unless defined?(ArrowDataset)
    tmpdir = Dir.mktmpdir
    begin
      path = File.join(tmpdir, "table.arrow")
      @table = build_table(visible: [
                             build_boolean_array([true, false, true]),
                             build_boolean_array([false, true, false, true]),
                           ],
                           point: [
                             build_int32_array([1, 2, 3]),
                             build_int32_array([-1, -2, -3, -4]),
                           ])
      @format = ArrowDataset::IPCFileFormat.new
      write_table(@table, path)
      factory = ArrowDataset::FileSystemDatasetFactory.new(@format)
      factory.file_system_uri = build_file_uri(path)
      @dataset = factory.finish
      builder = @dataset.begin_scan
      @scanner = builder.finish
      yield
    ensure
      # We have to ignore errors trying to remove the directory due to
      # the RecordBatchReader not closing files
      # (https://github.com/apache/arrow/issues/41771).
      # Also request GC first which should free any remaining RecordBatchReader
      # and close open files.
      GC.start
      FileUtils.remove_entry(tmpdir, force: true)
    end
  end

  def test_to_table
    assert_equal(@table, @scanner.to_table)
  end

  def test_record_batch_reader
    assert_equal(@table, @scanner.to_record_batch_reader.read_all)
  end
end
