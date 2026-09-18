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

class TestParquetReaderProperties < Test::Unit::TestCase
  def setup
    omit("Parquet is required") unless defined?(::Parquet)
    @properties = Parquet::ReaderProperties.new
  end

  def test_buffered_stream
    assert_false(@properties.buffered_stream_enabled?)
    @properties.enable_buffered_stream
    assert_true(@properties.buffered_stream_enabled?)
    @properties.disable_buffered_stream
    assert_false(@properties.buffered_stream_enabled?)
  end

  def test_buffer_size
    assert_equal(16 * 1024, @properties.buffer_size)
    @properties.buffer_size = 32 * 1024
    assert_equal(32 * 1024, @properties.buffer_size)
    assert_false(@properties.buffered_stream_enabled?)
  end
end
