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

class TestSelectKOptions < Test::Unit::TestCase
  include Helper::Buildable

  def setup
    @options = Arrow::SelectKOptions.new
  end

  def test_k
    assert_equal(-1, @options.k)
    @options.k = 3
    assert_equal(3, @options.k)
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

  def test_select_k_unstable_function
    input_array = build_int32_array([5, 2, 8, 1, 9, 3])
    args = [
      Arrow::ArrayDatum.new(input_array),
    ]
    @options.k = 3
    @options.add_sort_key(Arrow::SortKey.new("dummy", :descending))
    select_k_unstable_function = Arrow::Function.find("select_k_unstable")
    result = select_k_unstable_function.execute(args, @options).value
    assert_equal(build_uint64_array([4, 2, 0]), result)
  end
end

