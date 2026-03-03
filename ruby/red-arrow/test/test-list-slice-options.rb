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

class TestListSliceOptions < Test::Unit::TestCase
  def setup
    @options = Arrow::ListSliceOptions.new
  end

  def test_stop_property
    assert_equal(nil, @options.stop)
    @options.stop = 5
    assert_equal(5, @options.stop)
    @options.stop = nil
    assert_equal(nil, @options.stop)
  end

  def test_return_fixed_size_list_property
    assert_equal(nil, @options.return_fixed_size_list)
    @options.return_fixed_size_list = true
    assert_equal(true, @options.return_fixed_size_list)
    @options.return_fixed_size_list = false
    assert_equal(false, @options.return_fixed_size_list)
    @options.return_fixed_size_list = nil
    assert_equal(nil, @options.return_fixed_size_list)
  end

  def test_list_slice_function_without_stop
    args = [
      Arrow::ArrayDatum.new(Arrow::ListArray.new([:list, :int8], [[1, 2, 3], [4, 5]])),
    ]
    @options.start = 1
    @options.stop = nil
    list_slice_function = Arrow::Function.find("list_slice")
    result = list_slice_function.execute(args, @options).value
    assert_equal(Arrow::ListArray.new([:list, :int8], [[2, 3], [5]]),
                 result)
  end

  def test_list_slice_function_with_return_fixed_size_list_auto
    args = [
      Arrow::ArrayDatum.new(Arrow::FixedSizeListArray.new([:fixed_size_list, :int8, 2],
                                                          [[1, 2], [3, 4]])),
    ]
    @options.start = 1
    list_slice_function = Arrow::Function.find("list_slice")
    result = list_slice_function.execute(args, @options).value
    assert_equal(Arrow::FixedSizeListArray.new([:fixed_size_list, :int8, 1], [[2], [4]]),
                 result)
  end

  def test_list_slice_function_with_return_fixed_size_list_true
    args = [
      Arrow::ArrayDatum.new(Arrow::ListArray.new([:list, :int8], [[1, 2, 3], [4, 5]])),
    ]
    @options.start = 1
    @options.stop = 3
    @options.return_fixed_size_list = true
    list_slice_function = Arrow::Function.find("list_slice")
    result = list_slice_function.execute(args, @options).value
    assert_equal(Arrow::FixedSizeListArray.new([:fixed_size_list, :int8, 2], [[2, 3], [5, nil]]),
                 result)
  end

  def test_list_slice_function_with_return_fixed_size_list_false
    args = [
      Arrow::ArrayDatum.new(Arrow::FixedSizeListArray.new([:fixed_size_list, :int8, 2],
                                                          [[1, 2], [3, 4]])),
    ]
    @options.start = 1
    @options.return_fixed_size_list = false
    list_slice_function = Arrow::Function.find("list_slice")
    result = list_slice_function.execute(args, @options).value
    assert_equal(Arrow::ListArray.new([:list, :int8], [[2], [4]]),
                 result)
  end
end
