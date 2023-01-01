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

class TestDatasetFileSystemDatasetFactory < Test::Unit::TestCase
  include Helper::Buildable
  include Helper::Writable

  def setup
    omit("Arrow Dataset is required") unless defined?(ArrowDataset)
    Dir.mktmpdir do |tmpdir|
      @dir = tmpdir
      @format = ArrowDataset::IPCFileFormat.new
      @path1 = File.join(@dir, "table1.arrow")
      @table1 = build_table(visible: [
                              build_boolean_array([true, false, true]),
                              build_boolean_array([false, true, false, true]),
                            ],
                            point: [
                              build_int32_array([1, 2, 3]),
                              build_int32_array([-1, -2, -3, -4]),
                            ])
      write_table(@table1, @path1)
      @path2 = File.join(@dir, "table2.arrow")
      @table2 = build_table(visible: [
                              build_boolean_array([false, true]),
                              build_boolean_array([true]),
                            ],
                            point: [
                              build_int32_array([10]),
                              build_int32_array([-10, -20]),
                            ])
      write_table(@table2, @path2)
      yield
    end
  end

  def test_file_system
    factory = ArrowDataset::FileSystemDatasetFactory.new(@format)
    factory.file_system = Arrow::LocalFileSystem.new
    factory.add_path(File.expand_path(@path1))
    dataset = factory.finish
    assert_equal(@table1, dataset.to_table)
  end

  def test_file_system_uri
    factory = ArrowDataset::FileSystemDatasetFactory.new(@format)
    factory.file_system_uri = build_file_uri(@path1)
    dataset = factory.finish
    assert_equal(@table1, dataset.to_table)
  end

  def test_directory
    factory = ArrowDataset::FileSystemDatasetFactory.new(@format)
    factory.file_system_uri = build_file_uri(@dir)
    dataset = factory.finish
    assert_equal(@table1.concatenate([@table2]),
                 dataset.to_table)
  end

  sub_test_case("#finish") do
    def setup
      super do
        @factory = ArrowDataset::FileSystemDatasetFactory.new(@format)
        @factory.file_system_uri = build_file_uri(@path1)
        yield
      end
    end

    def test_schema
      options = ArrowDataset::FinishOptions.new
      options.schema = build_schema(visible: Arrow::BooleanDataType.new,
                                    point: Arrow::Int16DataType.new)
      dataset = @factory.finish(options)
      assert_equal(build_table(visible: [
                                 build_boolean_array([true, false, true]),
                                 build_boolean_array([false, true, false, true]),
                               ],
                               point: [
                                 build_int16_array([1, 2, 3]),
                                 build_int16_array([-1, -2, -3, -4]),
                               ]),
                   dataset.to_table)
    end

    def test_inspect_n_fragments
      options = ArrowDataset::FinishOptions.new
      options.inspect_n_fragments = -1
      dataset = @factory.finish(options)
      assert_equal(@table1, dataset.to_table)
    end

    def test_validate_fragments
      options = ArrowDataset::FinishOptions.new
      options.schema = build_schema(visible: Arrow::BooleanDataType.new,
                                    point: Arrow::Int16DataType.new)
      options.validate_fragments = true
      message = "[file-system-dataset-factory][finish]: " +
                "Invalid: Unable to merge: " +
                "Field point has incompatible types: int16 vs int32"
      error = assert_raise(Arrow::Error::Invalid) do
        @factory.finish(options)
      end
      assert_equal(message, error.message.lines(chomp: true).first)
    end
  end
end
