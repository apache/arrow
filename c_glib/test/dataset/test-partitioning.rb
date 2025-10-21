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

class TestDatasetPartitioning < Test::Unit::TestCase
  include Helper::Buildable

  def setup
    omit("Arrow Dataset is required") unless defined?(ArrowDataset)
  end

  def test_default
    assert_equal("directory",
                 ArrowDataset::Partitioning.create_default.type_name)
  end

  def test_directory
    schema = build_schema(year: Arrow::UInt16DataType.new)
    partitioning = ArrowDataset::DirectoryPartitioning.new(schema)
    assert_equal("directory", partitioning.type_name)
  end

  def test_directory_options
    schema = build_schema(year: Arrow::UInt16DataType.new)
    options = ArrowDataset::KeyValuePartitioningOptions.new
    options.segment_encoding = :none
    partitioning = ArrowDataset::DirectoryPartitioning.new(schema,
                                                           nil,
                                                           options)
    assert_equal("directory", partitioning.type_name)
  end

  def test_hive
    schema = build_schema(year: Arrow::UInt16DataType.new)
    partitioning = ArrowDataset::HivePartitioning.new(schema)
    assert_equal("hive", partitioning.type_name)
  end

  def test_hive_options
    schema = build_schema(year: Arrow::UInt16DataType.new)
    options = ArrowDataset::HivePartitioningOptions.new
    options.segment_encoding = :none
    options.null_fallback = "NULL"
    partitioning = ArrowDataset::HivePartitioning.new(schema,
                                                      nil,
                                                      options)
    assert_equal("NULL", partitioning.null_fallback)
  end
end
