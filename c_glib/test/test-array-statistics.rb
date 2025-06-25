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

class TestArrayStatistics < Test::Unit::TestCase
  include Helper::Buildable

  def setup
    omit("Parquet is required") unless defined?(::Parquet)

    Tempfile.create(["data", ".parquet"]) do |file|
      @file = file
      array = build_int64_array([nil, -(2 ** 32), 2 ** 32])
      @table = build_table("int64" => array)
      writer = Parquet::ArrowFileWriter.new(@table.schema, @file.path)
      chunk_size = 1024
      writer.write_table(@table, chunk_size)
      writer.close
      reader = Parquet::ArrowFileReader.new(@file.path)
      begin
        @statistics = reader.read_table.get_column_data(0).get_chunk(0).statistics
        yield
      ensure
        reader.unref
      end
    end
  end

  test("#has_null_count?") do
    assert do
      @statistics.has_null_count?
    end
  end

  test("#null_count") do
    assert_equal(1, @statistics.null_count)
  end

  test("#has_distinct_count?") do
    assert do
      !@statistics.has_distinct_count?
    end
  end

  test ("#distinct_count") do
    assert_equal(-1, @statistics.distinct_count)
  end
end
