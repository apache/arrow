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

class TestRankOptions < Test::Unit::TestCase
  include Helper::Buildable

  def setup
    @options = Arrow::Function.find("rank").default_options
  end

  def test_equal
    assert_equal(Arrow::SortOptions.new,
                 Arrow::SortOptions.new)
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

  def test_tiebreaker
    assert_equal(Arrow::RankTiebreaker::FIRST, @options.tiebreaker)
    @options.tiebreaker = :max
    assert_equal(Arrow::RankTiebreaker::MAX, @options.tiebreaker)
  end
end
