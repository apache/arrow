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

class TestTensor < Test::Unit::TestCase
  include Helper::Omittable

  def setup
    @raw_data = [
      1, 2,
      3, 4,

      5, 6,
      7, 8,

      9, 10,
      11, 12,
    ]
    data = Arrow::Buffer.new(@raw_data.pack("c*"))
    @shape = [3, 2, 2]
    strides = []
    names = ["a", "b", "c"]
    @tensor = Arrow::Tensor.new(Arrow::Int8DataType.new,
                                data,
                                @shape,
                                strides,
                                names)
  end

  def test_equal
    data = Arrow::Buffer.new(@raw_data.pack("c*"))
    strides = []
    names = ["a", "b", "c"]
    other_tensor = Arrow::Tensor.new(Arrow::Int8DataType.new,
                                     data,
                                     @shape,
                                     strides,
                                     names)
    assert_equal(@tensor,
                 other_tensor)
  end

  def test_value_data_type
    assert_equal(Arrow::Int8DataType, @tensor.value_data_type.class)
  end

  def test_value_type
    assert_equal(Arrow::Type::INT8, @tensor.value_type)
  end

  def test_buffer
    assert_equal(@raw_data, @tensor.buffer.data.to_s.unpack("c*"))
  end

  def test_shape
    require_gi_bindings(3, 1, 2)
    assert_equal(@shape, @tensor.shape)
  end

  def test_strides
    require_gi_bindings(3, 1, 2)
    assert_equal([4, 2, 1], @tensor.strides)
  end

  def test_n_dimensions
    assert_equal(@shape.size, @tensor.n_dimensions)
  end

  def test_dimension_name
    dimension_names = @tensor.n_dimensions.times.collect do |i|
      @tensor.get_dimension_name(i)
    end
    assert_equal(["a", "b", "c"],
                 dimension_names)
  end

  def test_size
    assert_equal(@raw_data.size, @tensor.size)
  end

  def test_mutable?
    assert do
      not @tensor.mutable?
    end
  end

  def test_contiguous?
    assert do
      @tensor.contiguous?
    end
  end

  def test_row_major?
    assert do
      @tensor.row_major?
    end
  end

  def test_column_major?
    assert do
      not @tensor.column_major?
    end
  end

  def test_io
    buffer = Arrow::ResizableBuffer.new(0)
    output = Arrow::BufferOutputStream.new(buffer)
    output.write_tensor(@tensor)
    input = Arrow::BufferInputStream.new(buffer)
    assert_equal(@tensor,
                 input.read_tensor(0))
  end
end
