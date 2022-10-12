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

class TensorTest < Test::Unit::TestCase
  sub_test_case("class methods") do
    sub_test_case(".new") do
      def setup
        @raw_tensor = [
          [
            [1, 2, 3, 4],
            [5, 6, 7, 8],
          ],
          [
            [9, 10, 11, 12],
            [13, 14, 15, 16],
          ],
          [
            [17, 18, 19, 20],
            [21, 22, 23, 24],
          ],
        ]
        @shape = [3, 2, 4]
        @strides = [8, 4, 1]
      end

      test("Array") do
        tensor = Arrow::Tensor.new(@raw_tensor)
        assert_equal({
                       value_data_type: Arrow::UInt8DataType.new,
                       buffer: @raw_tensor.flatten.pack("C*"),
                       shape: @shape,
                       strides: @strides,
                       dimension_names: ["", "", ""],
                     },
                     {
                       value_data_type: tensor.value_data_type,
                       buffer: tensor.buffer.data.to_s,
                       shape: tensor.shape,
                       strides: tensor.strides,
                       dimension_names: tensor.dimension_names,
                     })
      end

      test("Array, data_type: Symbol") do
        tensor = Arrow::Tensor.new(@raw_tensor, data_type: :int32)
        assert_equal({
                       value_data_type: Arrow::Int32DataType.new,
                       buffer: @raw_tensor.flatten.pack("l*"),
                       shape: @shape,
                       strides: @strides.collect {|x| x * 4},
                       dimension_names: ["", "", ""],
                     },
                     {
                       value_data_type: tensor.value_data_type,
                       buffer: tensor.buffer.data.to_s,
                       shape: tensor.shape,
                       strides: tensor.strides,
                       dimension_names: tensor.dimension_names,
                     })
      end

      test("Array, dimension_names: Array<String>") do
        tensor = Arrow::Tensor.new(@raw_tensor,
                                   dimension_names: ["a", "b", "c"])
        assert_equal({
                       value_data_type: Arrow::UInt8DataType.new,
                       buffer: @raw_tensor.flatten.pack("C*"),
                       shape: @shape,
                       strides: @strides,
                       dimension_names: ["a", "b", "c"],
                     },
                     {
                       value_data_type: tensor.value_data_type,
                       buffer: tensor.buffer.data.to_s,
                       shape: tensor.shape,
                       strides: tensor.strides,
                       dimension_names: tensor.dimension_names,
                     })
      end

      test("Array, dimension_names: Array<Symbol>") do
        tensor = Arrow::Tensor.new(@raw_tensor,
                                   dimension_names: [:a, :b, :c])
        assert_equal({
                       value_data_type: Arrow::UInt8DataType.new,
                       buffer: @raw_tensor.flatten.pack("C*"),
                       shape: @shape,
                       strides: @strides,
                       dimension_names: ["a", "b", "c"],
                     },
                     {
                       value_data_type: tensor.value_data_type,
                       buffer: tensor.buffer.data.to_s,
                       shape: tensor.shape,
                       strides: tensor.strides,
                       dimension_names: tensor.dimension_names,
                     })
      end
    end
  end

  sub_test_case("instance methods") do
    def setup
      raw_data = [
        1, 2,
        3, 4,

        5, 6,
        7, 8,

        9, 10,
        11, 12,
      ]
      data = Arrow::Buffer.new(raw_data.pack("c*").freeze)
      shape = [3, 2, 2]
      strides = []
      names = ["a", "b", "c"]
      @tensor = Arrow::Tensor.new(:int8,
                                  data,
                                  shape,
                                  strides,
                                  names)
    end

    sub_test_case("#==") do
      test("Arrow::Tensor") do
        assert do
          @tensor == @tensor
        end
      end

      test("not Arrow::Tensor") do
        assert do
          not (@tensor == 29)
        end
      end
    end
  end
end
