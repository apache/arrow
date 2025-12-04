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

class TestMapLookupOptions < Test::Unit::TestCase
  include Helper::Buildable

  def setup
    @query_key = Arrow::Int32Scalar.new(1)
    @options = Arrow::MapLookupOptions.new(@query_key,
                                           Arrow::MapLookupOccurrence::FIRST)
  end

  def test_query_key_property
    assert_equal(@query_key, @options.query_key)
    new_query_key = Arrow::Int32Scalar.new(2)
    @options.query_key = new_query_key
    assert_equal(new_query_key, @options.query_key)
  end

  def test_occurrence_property
    assert_equal(Arrow::MapLookupOccurrence::FIRST, @options.occurrence)
    @options.occurrence = :last
    assert_equal(Arrow::MapLookupOccurrence::LAST, @options.occurrence)
    @options.occurrence = :all
    assert_equal(Arrow::MapLookupOccurrence::ALL, @options.occurrence)
    @options.occurrence = :first
    assert_equal(Arrow::MapLookupOccurrence::FIRST, @options.occurrence)
  end

  def build_map_with_duplicate_keys
  end

  def test_map_lookup_function
    map_array = build_map_array(Arrow::Int32DataType.new,
                                Arrow::StringDataType.new,
                                [[
                                  [1, "first_one"],
                                  [2, "two"],
                                  [1, nil],
                                  [3, "three"],
                                  [1, "second_one"],
                                  [1, "last_one"],
                                ]])
    args = [Arrow::ArrayDatum.new(map_array)]
    map_lookup_function = Arrow::Function.find("map_lookup")
    @options.query_key = Arrow::Int32Scalar.new(1)

    @options.occurrence = :first
    result = map_lookup_function.execute(args, @options).value
    assert_equal(build_string_array(["first_one"]), result)

    @options.occurrence = :last
    result = map_lookup_function.execute(args, @options).value
    assert_equal(build_string_array(["last_one"]), result)

    @options.occurrence = :all
    result = map_lookup_function.execute(args, @options).value
    assert_equal(build_list_array(Arrow::StringDataType.new,
                                  [["first_one", nil, "second_one", "last_one"]]),
                 result)
  end
end
