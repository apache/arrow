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

class TestParquetWriterProperties < Test::Unit::TestCase
  def setup
    omit("Parquet is required") unless defined?(::Parquet)
    @properties = Parquet::WriterProperties.new
  end

  def test_compression
    @properties.set_compression(:gzip)
    assert_equal(Arrow::CompressionType::GZIP,
                 @properties.get_compression_path("not-specified"))
  end

  def test_compression_with_path
    @properties.set_compression(:gzip, "column")
    assert_equal([
                   Arrow::CompressionType::GZIP,
                   Arrow::CompressionType::UNCOMPRESSED,
                 ],
                 [
                   @properties.get_compression_path("column"),
                   @properties.get_compression_path("not-specified"),
                 ])
  end

  def test_enable_dictionary
    @properties.enable_dictionary
    assert_equal(true,
                 @properties.dictionary_enabled?("not-specified"))
  end

  def test_enable_dictionary_with_path
    @properties.disable_dictionary
    @properties.enable_dictionary("column")
    assert_equal([
                   true,
                   false,
                 ],
                 [
                   @properties.dictionary_enabled?("column"),
                   @properties.dictionary_enabled?("not-specified"),
                 ])
  end

  def test_disable_dictionary
    @properties.disable_dictionary
    assert_equal(false,
                 @properties.dictionary_enabled?("not-specified"))
  end

  def test_disable_dictionary_with_path
    @properties.enable_dictionary
    @properties.disable_dictionary("column")
    assert_equal([
                   false,
                   true,
                 ],
                 [
                   @properties.dictionary_enabled?("column"),
                   @properties.dictionary_enabled?("not-specified"),
                 ])
  end

  def test_dictionary_page_size_limit
    @properties.dictionary_page_size_limit = 4096
    assert_equal(4096,
                 @properties.dictionary_page_size_limit)
  end

  def test_batch_size
    @properties.batch_size = 100
    assert_equal(100,
                 @properties.batch_size)
  end

  def test_data_page_size
    @properties.data_page_size = 128
    assert_equal(128,
                 @properties.data_page_size)
  end

  def test_max_row_group_length
    @properties.max_row_group_length = 1024
    assert_equal(1024,
                 @properties.max_row_group_length)
  end
end
