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

class TestParquetArrowFileWriter < Test::Unit::TestCase
  include Helper::Buildable

  def setup
    omit("Parquet is required") unless defined?(::Parquet)
    @file = Tempfile.open(["data", ".parquet"])
  end

  def test_write
    enabled_values = [true, nil, false, true]
    table = build_table("enabled" => build_boolean_array(enabled_values))
    chunk_size = 2

    writer = Parquet::ArrowFileWriter.new(table.schema, @file.path)
    writer.write_table(table, chunk_size)
    writer.close

    reader = Parquet::ArrowFileReader.new(@file.path)
    reader.use_threads = true
    assert_equal([
                   enabled_values.length / chunk_size,
                   table,
                 ],
                 [
                   reader.n_row_groups,
                   reader.read_table,
                 ])
  end
end
