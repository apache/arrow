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

class TestDatasetScanOptions < Test::Unit::TestCase
  def setup
    omit("Arrow Dataset is required") unless defined?(ArrowDataset)
    @schema = Arrow::Schema.new([])
    @scan_options = ArrowDataset::ScanOptions.new(@schema)
  end

  def test_schema
    assert_equal(@schema,
                 @scan_options.schema)
  end

  def test_batch_size
    assert_equal(1<<20,
                 @scan_options.batch_size)
    @scan_options.batch_size = 42
    assert_equal(42,
                 @scan_options.batch_size)
  end

  def test_replace_schema
    other_schema = Arrow::Schema.new([Arrow::Field.new("visible", Arrow::BooleanDataType.new)])
    other_scan_options = @scan_options.replace_schema(other_schema)
    assert_not_equal(@schema, other_scan_options.schema)
    assert_equal(other_schema, other_scan_options.schema)
  end
end
