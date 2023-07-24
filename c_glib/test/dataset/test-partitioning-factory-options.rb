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

class TestDatasetPartitioningFactoryOptions < Test::Unit::TestCase
  include Helper::Buildable

  def setup
    omit("Arrow Dataset is required") unless defined?(ArrowDataset)
    @options = ArrowDataset::PartitioningFactoryOptions.new
  end

  def test_infer_dictionary
    assert_false(@options.infer_dictionary?)
    @options.infer_dictionary = true
    assert_true(@options.infer_dictionary?)
  end

  def test_schema
    assert_nil(@options.schema)
    schema = build_schema(year: Arrow::UInt16DataType.new)
    @options.schema = schema
    assert_equal(schema, @options.schema)
  end

  def test_segment_encoding
    assert_equal(ArrowDataset::SegmentEncoding::NONE,
                 @options.segment_encoding)
    @options.segment_encoding = :uri
    assert_equal(ArrowDataset::SegmentEncoding::URI,
                 @options.segment_encoding)
  end
end
