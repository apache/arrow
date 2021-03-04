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

class TestSortOptions < Test::Unit::TestCase
  include Helper::Buildable

  def test_new
    sort_keys = [
      Arrow::SortKey.new("column1", :ascending),
      Arrow::SortKey.new("column2", :descending),
    ]
    options = Arrow::SortOptions.new(sort_keys)
    assert_equal(sort_keys, options.sort_keys)
  end

  def test_add_sort_key
    options = Arrow::SortOptions.new
    options.add_sort_key(Arrow::SortKey.new("column1", :ascending))
    options.add_sort_key(Arrow::SortKey.new("column2", :descending))
    assert_equal([
                   Arrow::SortKey.new("column1", :ascending),
                   Arrow::SortKey.new("column2", :descending),
                 ],
                 options.sort_keys)
  end

  def test_set_sort_keys
    options = Arrow::SortOptions.new([Arrow::SortKey.new("column3", :ascending)])
    sort_keys = [
      Arrow::SortKey.new("column1", :ascending),
      Arrow::SortKey.new("column2", :descending),
    ]
    options.sort_keys = sort_keys
    assert_equal(sort_keys, options.sort_keys)
  end

  def test_equal
    sort_keys = [
      Arrow::SortKey.new("column1", :ascending),
      Arrow::SortKey.new("column2", :descending),
    ]
    assert_equal(Arrow::SortOptions.new(sort_keys),
                 Arrow::SortOptions.new(sort_keys))
  end
end
