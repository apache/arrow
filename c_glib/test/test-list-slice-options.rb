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
  include Helper::Buildable

  def setup
    @options = Arrow::ListSliceOptions.new
  end

  def test_start_property
    assert_equal(0, @options.start)
    @options.start = 2
    assert_equal(2, @options.start)
  end

  def test_stop_property
    assert_equal(Arrow::LIST_SLICE_OPTIONS_STOP_UNSPECIFIED, @options.stop)
    @options.stop = 5
    assert_equal(5, @options.stop)
    @options.stop = Arrow::LIST_SLICE_OPTIONS_STOP_UNSPECIFIED
    assert_equal(LIST_SLICE_OPTIONS_STOP_UNSPECIFIED, @options.stop)
  end

  def test_step_property
    assert_equal(1, @options.step)
    @options.step = 2
    assert_equal(2, @options.step)
  end

  def test_return_fixed_size_list_property
    assert_equal(Arrow::ListSliceReturnFixedSizeList::AUTO,
                 @options.return_fixed_size_list)
    @options.return_fixed_size_list = :true
    assert_equal(Arrow::ListSliceReturnFixedSizeList::TRUE,
                 @options.return_fixed_size_list)
    @options.return_fixed_size_list = :false
    assert_equal(Arrow::ListSliceReturnFixedSizeList::FALSE,
                 @options.return_fixed_size_list)
    @options.return_fixed_size_list = :auto
    assert_equal(Arrow::ListSliceReturnFixedSizeList::AUTO,
                 @options.return_fixed_size_list)
  end

  def test_list_slice_function_with_step
    args = [
      Arrow::ArrayDatum.new(build_list_array(Arrow::Int8DataType.new,
                                             [[1, 2, 3, 4, 5], [6, 7, 8, 9]])),
    ]
    @options.step = 2
    list_slice_function = Arrow::Function.find("list_slice")
    result = list_slice_function.execute(args, @options).value
    assert_equal(build_list_array(Arrow::Int8DataType.new, [[1, 3, 5], [6, 8]]),
                 result)
  end

  def test_list_slice_function_without_stop
    args = [
      Arrow::ArrayDatum.new(build_list_array(Arrow::Int8DataType.new,
                                             [[1, 2, 3], [4, 5]])),
    ]
    @options.start = 1
    @options.stop = Arrow::LIST_SLICE_OPTIONS_STOP_UNSPECIFIED
    list_slice_function = Arrow::Function.find("list_slice")
    result = list_slice_function.execute(args, @options).value
    assert_equal(build_list_array(Arrow::Int8DataType.new, [[2, 3], [5]]),
                 result)
  end

  def test_list_slice_function_with_return_fixed_size_list_auto
    args = [
      Arrow::ArrayDatum.new(build_fixed_size_list_array(Arrow::Int8DataType.new, 2,
                                                        [[1, 2], [3, 4]])),
    ]
    @options.start = 1
    list_slice_function = Arrow::Function.find("list_slice")
    result = list_slice_function.execute(args, @options).value
    assert_equal(build_fixed_size_list_array(Arrow::Int8DataType.new, 1, [[2], [4]]),
                 result)
  end

  def test_list_slice_function_with_return_fixed_size_list_true
    args = [
      Arrow::ArrayDatum.new(build_list_array(Arrow::Int8DataType.new,
                                             [[1, 2, 3], [4, 5]])),
    ]
    @options.start = 1
    @options.stop = 3
    @options.return_fixed_size_list = :true
    list_slice_function = Arrow::Function.find("list_slice")
    result = list_slice_function.execute(args, @options).value
    assert_equal(build_fixed_size_list_array(Arrow::Int8DataType.new, 2, [[2, 3], [5, nil]]),
                 result)
  end

  def test_list_slice_function_with_return_fixed_size_list_false
    args = [
      Arrow::ArrayDatum.new(build_fixed_size_list_array(Arrow::Int8DataType.new, 2,
                                                        [[1, 2], [3, 4]])),
    ]
    @options.start = 1
    @options.return_fixed_size_list = :false
    list_slice_function = Arrow::Function.find("list_slice")
    result = list_slice_function.execute(args, @options).value
    assert_equal(build_list_array(Arrow::Int8DataType.new, [[2], [4]]),
                 result)
  end
end
