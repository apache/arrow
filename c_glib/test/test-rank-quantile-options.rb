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

class TestRankQuantileOptions < Test::Unit::TestCase
  include Helper::Buildable

  def setup
    @options = Arrow::RankQuantileOptions.new
  end

  def test_sort_keys
    sort_keys = [
      Arrow::SortKey.new("column1", :ascending),
      Arrow::SortKey.new("column2", :descending),
    ]
    @options.sort_keys = sort_keys
    assert_equal(sort_keys, @options.sort_keys)
  end

  def test_add_sort_key
    @options.add_sort_key(Arrow::SortKey.new("column1", :ascending))
    @options.add_sort_key(Arrow::SortKey.new("column2", :descending))
    assert_equal([
                   Arrow::SortKey.new("column1", :ascending),
                   Arrow::SortKey.new("column2", :descending),
                 ],
                 @options.sort_keys)
  end

  def test_null_placement
    assert_equal(Arrow::NullPlacement::AT_END, @options.null_placement)
    @options.null_placement = :at_start
    assert_equal(Arrow::NullPlacement::AT_START, @options.null_placement)
  end

  def test_rank_quantile_function
    args = [
      Arrow::ArrayDatum.new(build_int32_array([nil, 1, nil, 2, nil])),
    ]
    @options.null_placement = :at_start
    rank_quantile_function = Arrow::Function.find("rank_quantile")
    result = rank_quantile_function.execute(args, @options).value
    expected = build_double_array([0.3, 0.7, 0.3, 0.9, 0.3])
    assert_equal(expected, result)
  end
end

