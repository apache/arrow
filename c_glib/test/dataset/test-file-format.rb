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

class TestDatasetFileFormat < Test::Unit::TestCase
  def setup
    omit("Arrow Dataset is required") unless defined?(ArrowDataset)
  end

  def test_csv
    assert_equal("csv", ArrowDataset::CSVFileFormat.new.type_name)
  end

  def test_ipc
    assert_equal("ipc", ArrowDataset::IPCFileFormat.new.type_name)
  end

  def test_parquet
    assert_equal("parquet", ArrowDataset::ParquetFileFormat.new.type_name)
  end
end
