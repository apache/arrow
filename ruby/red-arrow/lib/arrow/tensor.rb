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

require_relative "raw-tensor-converter"

module Arrow
  class Tensor
    alias_method :initialize_raw, :initialize
    # Creates a new {Arrow::Tensor}.
    #
    # @overload initialize(raw_tensor, data_type: nil, shape: nil, dimension_names: nil)
    #
    #   @param raw_tensor [::Array<Numeric>] The tensor represented as a
    #     raw `Array` (not `Arrow::Array`) and `Numeric`s. You can
    #     pass a nested `Array` for a multi-dimensional tensor.
    #
    #   @param data_type [Arrow::DataType, String, Symbol, ::Array<String>,
    #     ::Array<Symbol>, Hash, nil] The element data type of the tensor.
    #
    #     If you specify `nil`, data type is guessed from `raw_tensor`.
    #
    #     See {Arrow::DataType.resolve} for how to specify data type.
    #
    #   @param shape [::Array<Integer>, nil] The array of dimension sizes.
    #
    #     If you specify `nil`, shape is guessed from `raw_tensor`.
    #
    #   @param dimension_names [::Array<String>, ::Array<Symbol>, nil]
    #     The array of the dimension names.
    #
    #     If you specify `nil`, all dimensions have empty names.
    #
    #   @example Create a tensor from Ruby's Array
    #     raw_tensor = [
    #       [
    #         [1, 2, 3, 4],
    #         [5, 6, 7, 8],
    #       ],
    #       [
    #         [9, 10, 11, 12],
    #         [13, 14, 15, 16],
    #       ],
    #       [
    #         [17, 18, 19, 20],
    #         [21, 22, 23, 24],
    #       ],
    #     ]
    #     Arrow::Tensor.new(raw_tensor)
    #
    #   @since 10.0.0
    #
    # @overload initialize(data_type, data, shape, strides, dimension_names)
    #
    #   @param data_type [Arrow::DataType, String, Symbol, ::Array<String>,
    #     ::Array<Symbol>, Hash] The element data type of the tensor.
    #
    #     See {Arrow::DataType.resolve} how to specify data type.
    #
    #   @param data [Arrow::Buffer, String] The data of the tensor.
    #
    #   @param shape [::Array<Integer>] The array of dimension sizes.
    #
    #   @param strides [::Array<Integer>, nil] The array of strides which
    #     is the number of bytes between two adjacent elements in each
    #     dimension.
    #
    #     If you specify `nil` or an empty `Array`, strides are
    #     guessed from `data_type` and `data`.
    #
    #   @param dimension_names [::Array<String>, ::Array<Symbol>, nil]
    #     The array of the dimension names.
    #
    #     If you specify `nil`, all dimensions doesn't have their names.
    #
    #   @example Create a table from Arrow::Buffer
    #     raw_data = [
    #       1, 2,
    #       3, 4,
    #
    #       5, 6,
    #       7, 8,
    #
    #       9, 10,
    #       11, 12,
    #     ]
    #     data = Arrow::Buffer.new(raw_data.pack("c*").freeze)
    #     shape = [3, 2, 2]
    #     strides = []
    #     names = ["a", "b", "c"]
    #     Arrow::Tensor.new(:int8, data, shape, strides, names)
    def initialize(*args,
                   data_type: nil,
                   data: nil,
                   shape: nil,
                   strides: nil,
                   dimension_names: nil)
      n_args = args.size
      case n_args
      when 1
        converter = RawTensorConverter.new(args[0],
                                           data_type: data_type,
                                           shape: shape,
                                           strides: strides,
                                           dimension_names: dimension_names)
        data_type = converter.data_type
        data = converter.data
        shape = converter.shape
        strides = converter.strides
        dimension_names = converter.dimension_names
      when 0, 2..5
        data_type = args[0] || data_type
        data = args[1] || data
        shape = args[2] || shape
        strides = args[3] || strides
        dimension_names = args[4] || dimension_names
        if data_type.nil?
          raise ArgumentError, "data_type: is missing: #{data.inspect}"
        end
      else
        message = "wrong number of arguments (given #{n_args}, expected 0..5)"
        raise ArgumentError, message
      end
      initialize_raw(DataType.resolve(data_type),
                     data,
                     shape,
                     strides,
                     dimension_names)
    end

    def dimension_names
      n_dimensions.times.collect do |i|
        get_dimension_name(i)
      end
    end

    def to_arrow
      self
    end

    def to_arrow_array
      if n_dimensions != 1
        raise RangeError, "must be 1 dimensional tensor: #{shape.inspect}"
      end
      value_data_type.array_class.new(size,
                                      buffer,
                                      nil,
                                      0)
    end

    def to_arrow_chunked_array
      ChunkedArray.new([to_arrow_array])
    end
  end
end
