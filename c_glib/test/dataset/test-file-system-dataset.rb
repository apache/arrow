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

class TestDatasetFileSystemDataset < Test::Unit::TestCase
  include Helper::Buildable
  include Helper::Readable

  def setup
    omit("Arrow Dataset is required") unless defined?(ArrowDataset)
    Dir.mktmpdir do |tmpdir|
      @dir = tmpdir
      @format = ArrowDataset::IPCFileFormat.new
      @factory = ArrowDataset::FileSystemDatasetFactory.new(@format)
      @file_system = Arrow::LocalFileSystem.new
      @factory.file_system = @file_system
      partitioning_schema = build_schema(label: Arrow::StringDataType.new)
      @partitioning =
        ArrowDataset::DirectoryPartitioning.new(partitioning_schema)
      @factory.partitioning = @partitioning
      yield
    end
  end

  def test_type_name
    dataset = @factory.finish
    assert_equal("filesystem", dataset.type_name)
  end

  def test_format
    dataset = @factory.finish
    assert_equal(@format, dataset.format)
  end

  def test_file_system
    dataset = @factory.finish
    assert_equal(@file_system, dataset.file_system)
  end

  def test_partitioning
    dataset = @factory.finish
    assert_equal(@partitioning, dataset.partitioning)
  end

  def test_read_write
    table = build_table(label: build_string_array(["a", "a", "b", "c"]),
                        count: build_int32_array([1, 10, 2, 3]))
    table_reader = Arrow::TableBatchReader.new(table)
    scanner_builder = ArrowDataset::ScannerBuilder.new(table_reader)
    scanner = scanner_builder.finish
    options = ArrowDataset::FileSystemDatasetWriteOptions.new
    options.file_write_options = @format.default_write_options
    options.file_system = Arrow::LocalFileSystem.new
    options.base_dir = @dir
    options.base_name_template = "{i}.arrow"
    options.partitioning = @partitioning
    ArrowDataset::FileSystemDataset.write_scanner(scanner, options)
    Find.find(@dir) do |path|
      @factory.add_path(path) if File.file?(path)
    end
    @factory.partition_base_dir = @dir
    dataset = @factory.finish
    assert_equal(build_table(count: [
                               build_int32_array([1, 10]),
                               build_int32_array([2]),
                               build_int32_array([3]),
                             ],
                             label: [
                               build_string_array(["a", "a"]),
                               build_string_array(["b"]),
                               build_string_array(["c"]),
                             ]),
                 dataset.to_table)
  end
end
