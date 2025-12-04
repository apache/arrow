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

class TestPartitionNthOptions < Test::Unit::TestCase
  include Helper::Buildable

  def setup
    @options = Arrow::PartitionNthOptions.new
  end

  def test_pivot_property
    assert_equal(0, @options.pivot)
    @options.pivot = 2
    assert_equal(2, @options.pivot)
  end

  def test_null_placement_property
    assert_equal(Arrow::NullPlacement::AT_END, @options.null_placement)
    @options.null_placement = :at_start
    assert_equal(Arrow::NullPlacement::AT_START, @options.null_placement)
    @options.null_placement = :at_end
    assert_equal(Arrow::NullPlacement::AT_END, @options.null_placement)
  end

  def test_partition_nth_indices_function
    args = [
      Arrow::ArrayDatum.new(build_int32_array([5, 1, 4, 2, nil, 3])),
    ]
    @options.pivot = 2
    partition_nth_indices_function = Arrow::Function.find("partition_nth_indices")
    result = partition_nth_indices_function.execute(args, @options).value
    assert_equal(5, result.get_value(2))
    @options.null_placement = :at_start
    result = partition_nth_indices_function.execute(args, @options).value
    assert_equal(5, result.get_value(3))
  end
end

