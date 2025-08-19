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
  def setup
    data = Tempfile.create(["red-parquet", ".parquet"]) do |file|
      table = Arrow::Table.new(int64: [nil, -(2 ** 32), 2 ** 32])
      table.save(file)
      File.read(file, mode: "rb")
    end
    loaded_table = Arrow::Table.load(Arrow::Buffer.new(data),
                                     format: :parquet)
    @statistics = loaded_table[:int64].data.chunks[0].statistics
  end

  def test_distinct_count
    assert do
      not @statistics.has_distinct_count?
    end
    assert do
      not @statistics.distinct_count_exact?
    end
    assert_nil(@statistics.distinct_count)
  end
end
