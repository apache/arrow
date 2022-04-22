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

class TestParquetStatistics < Test::Unit::TestCase
  include Helper::Buildable

  def setup
    omit("Parquet is required") unless defined?(::Parquet)
    @file = Tempfile.open(["data", ".parquet"])
    @table = build_table("int32" => build_int32_array([nil, 2, 2, 9]))
    writer = Parquet::ArrowFileWriter.new(@table.schema, @file.path)
    chunk_size = 1024
    writer.write_table(@table, chunk_size)
    writer.close
    reader = Parquet::ArrowFileReader.new(@file.path)
    @statistics = reader.metadata.get_row_group(0).get_column_chunk(0).statistics
  end

  test("#==") do
    omit("parquet::Statistics::Equals() is broken.")
    reader = Parquet::ArrowFileReader.new(@file.path)
    other_statistics =
      reader.metadata.get_row_group(0).get_column_chunk(0).statistics
    assert do
      @statistics == other_statistics
    end
  end

  test("#has_n_nulls?") do
    assert do
      @statistics.has_n_nulls?
    end
  end

  test("#n_nulls") do
    assert_equal(1, @statistics.n_nulls)
  end

  test("#has_n_distinct_values?") do
    assert do
      @statistics.has_n_distinct_values?
    end
  end

  test("#n_distinct_values") do
    # It seems that writer doesn't support this.
    assert_equal(0, @statistics.n_distinct_values)
  end

  test("#n_values") do
    assert_equal(3, @statistics.n_values)
  end

  test("#has_min_max?") do
    assert do
      @statistics.has_min_max?
    end
  end
end
