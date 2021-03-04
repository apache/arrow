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

class TestSortIndices < Test::Unit::TestCase
  include Helper::Buildable

  def test_array
    array = build_int16_array([nil, 1, 0, nil, 4, 3])
    assert_equal(build_uint64_array([2, 1, 5, 4, 0, 3]),
                 array.sort_indices(:ascending))
  end

  def test_chunked_array
    arrays = [
      build_int16_array([1]),
      build_int16_array([0, 4, -3]),
    ]
    chunked_array = Arrow::ChunkedArray.new(arrays)
    assert_equal(build_uint64_array([3, 1, 0, 2]),
                 chunked_array.sort_indices(:ascending))
  end

  def test_record_batch
    columns = {
      column1: build_int16_array([  1,   0,   4,    4, -3,   1]),
      column2: build_string_array(["a", "a", "b", "c", "d", "a"]),
    }
    record_batch = build_record_batch(columns)
    sort_keys = [
      Arrow::SortKey.new("column1", :ascending),
      Arrow::SortKey.new("column2", :descending),
    ]
    options = Arrow::SortOptions.new(sort_keys)
    assert_equal(build_uint64_array([4, 1, 0, 5, 3, 2]),
                 record_batch.sort_indices(options))
  end

  def test_table
    raw_array1 = [ 1,   0,   4,    4, -3,   1]
    raw_array2 = ["a", "a", "b", "c", "d", "a"]
    columns = {
      column1: Arrow::ChunkedArray.new([build_int16_array(raw_array1[0...1]),
                                        build_int16_array(raw_array1[1...3]),
                                        build_int16_array(raw_array1[3..-1])]),
      column2: Arrow::ChunkedArray.new([build_string_array(raw_array2[0...2]),
                                        build_string_array(raw_array2[2..-1])]),
    }
    table = build_table(columns)
    options = Arrow::SortOptions.new
    options.add_sort_key(Arrow::SortKey.new("column1", :ascending))
    options.add_sort_key(Arrow::SortKey.new("column2", :descending))
    assert_equal(build_uint64_array([4, 1, 0, 5, 3, 2]),
                 table.sort_indices(options))
  end
end
