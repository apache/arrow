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

class TestFixedShapeTensorDataType < Test::Unit::TestCase
  def setup
    @data_type = Arrow::FixedShapeTensorDataType.new(Arrow::UInt64DataType.new,
                                                     [3, 4],
                                                     [1, 0],
                                                     ["x", "y"])
  end

  def test_id
    assert_equal(Arrow::Type::EXTENSION, @data_type.id)
  end

  def test_name
    assert_equal(["extension", "arrow.fixed_shape_tensor"],
                 [@data_type.name, @data_type.extension_name])
  end

  def test_n_dimensions
    assert_equal(2, @data_type.n_dimensions)
  end

  def test_shape
    assert_equal([3, 4], @data_type.shape)
  end

  def test_permutation
    assert_equal([1, 0], @data_type.permutation)
  end

  def test_strides
    assert_equal([8, 32], @data_type.strides)
  end

  def test_dim_names
    assert_equal(["x", "y"], @data_type.dim_names)
  end

  def test_to_s
    assert do
      @data_type.to_s.start_with?("extension<arrow.fixed_shape_tensor")
    end
  end

  def test_nil_permutation
    data_type = Arrow::FixedShapeTensorDataType.new(Arrow::UInt64DataType.new,
                                                    [3, 4],
                                                    nil,
                                                    ["x", "y"])
    assert_equal([[], [32, 8]],
                 [data_type.permutation, data_type.strides])
  end

  def test_nil_dim_names
    data_type = Arrow::FixedShapeTensorDataType.new(Arrow::UInt64DataType.new,
                                                    [3, 4],
                                                    [0, 1],
                                                    nil)
    assert_equal([], data_type.dim_names)
  end

  def test_mismatch_permutation_size
    message =
      "[fixed-shape-tensor][new]: Invalid: " +
      "permutation size must match shape size. " +
      "Expected: 2 Got: 1"
    error = assert_raise(Arrow::Error::Invalid) do
      Arrow::FixedShapeTensorDataType.new(Arrow::UInt64DataType.new,
                                          [3, 4],
                                          [1],
                                          ["x", "y"])
    end
    assert_equal(message,
                 error.message.lines.first.chomp)
  end

  def test_mismatch_dim_names_size
    message =
      "[fixed-shape-tensor][new]: Invalid: " +
      "dim_names size must match shape size. " +
      "Expected: 2 Got: 1"
    error = assert_raise(Arrow::Error::Invalid) do
      Arrow::FixedShapeTensorDataType.new(Arrow::UInt64DataType.new,
                                          [3, 4],
                                          [],
                                          ["x"])
    end
    assert_equal(message,
                 error.message.lines.first.chomp)
  end
end
