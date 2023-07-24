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

module Arrow
  class Array
    include Enumerable

    include ArrayComputable
    include GenericFilterable
    include GenericTakeable
    include InputReferable

    class << self
      def new(*args)
        _builder_class = builder_class
        return super if _builder_class.nil?
        return super unless _builder_class.buildable?(args)
        _builder_class.build(*args)
      end

      def builder_class
        builder_class_name = "#{name}Builder"
        return nil unless const_defined?(builder_class_name)
        const_get(builder_class_name)
      end

      # @api private
      def try_convert(value)
        case value
        when ::Array
          begin
            new(value)
          rescue ArgumentError
            nil
          end
        else
          if value.respond_to?(:to_arrow_array)
            begin
              value.to_arrow_array
            rescue RangeError
              nil
            end
          else
            nil
          end
        end
      end
    end

    # @param i [Integer]
    #   The index of the value to be gotten.
    #
    #   You can specify negative index like for `::Array#[]`.
    #
    # @return [Object, nil]
    #   The `i`-th value.
    #
    #   `nil` for NULL value or out of range `i`.
    def [](i)
      i += length if i < 0
      return nil if i < 0 or i >= length
      if null?(i)
        nil
      else
        get_value(i)
      end
    end

    # @param other [Arrow::Array] The array to be compared.
    # @param options [Arrow::EqualOptions, Hash] (nil)
    #   The options to custom how to compare.
    #
    # @return [Boolean]
    #   `true` if both of them have the same data, `false` otherwise.
    #
    # @since 5.0.0
    def equal_array?(other, options=nil)
      equal_options(other, options)
    end

    def each
      return to_enum(__method__) unless block_given?

      length.times do |i|
        yield(self[i])
      end
    end

    def reverse_each
      return to_enum(__method__) unless block_given?

      (length - 1).downto(0) do |i|
        yield(self[i])
      end
    end

    def to_arrow
      self
    end

    def to_arrow_array
      self
    end

    def to_arrow_chunked_array
      ChunkedArray.new([self])
    end

    alias_method :value_data_type_raw, :value_data_type
    def value_data_type
      @value_data_type ||= value_data_type_raw
    end

    def to_a
      values
    end

    alias_method :is_in_raw, :is_in
    def is_in(values)
      case values
      when ::Array
        if self.class.builder_class.buildable?([values])
          values = self.class.new(values)
        else
          values = self.class.new(value_data_type, values)
        end
        is_in_raw(values)
      when ChunkedArray
        is_in_chunked_array(values)
      else
        is_in_raw(values)
      end
    end

    # @api private
    alias_method :concatenate_raw, :concatenate
    # Concatenates the given other arrays to the array.
    #
    # @param other_arrays [::Array, Arrow::Array] The arrays to be
    #   concatenated.
    #
    #   Each other array is processed by {#resolve} before they're
    #   concatenated.
    #
    # @example Raw Ruby Array
    #   array = Arrow::Int32Array.new([1])
    #   array.concatenate([2, 3], [4]) # => Arrow::Int32Array.new([1, 2, 3, 4])
    #
    # @example Arrow::Array
    #   array = Arrow::Int32Array.new([1])
    #   array.concatenate(Arrow::Int32Array.new([2, 3]),
    #                     Arrow::Int8Array.new([4])) # => Arrow::Int32Array.new([1, 2, 3, 4])
    #
    # @since 4.0.0
    def concatenate(*other_arrays)
      other_arrays = other_arrays.collect do |other_array|
        resolve(other_array)
      end
      concatenate_raw(other_arrays)
    end

    # Concatenates the given other array to the array.
    #
    # If you have multiple arrays to be concatenated, you should use
    # {#concatenate} to concatenate multiple arrays at once.
    #
    # @param other_array [::Array, Arrow::Array] The array to be concatenated.
    #
    #   `@other_array` is processed by {#resolve} before it's
    #   concatenated.
    #
    # @example Raw Ruby Array
    #   Arrow::Int32Array.new([1]) + [2, 3] # => Arrow::Int32Array.new([1, 2, 3])
    #
    # @example Arrow::Array
    #   Arrow::Int32Array.new([1]) +
    #     Arrow::Int32Array.new([2, 3]) # => Arrow::Int32Array.new([1, 2, 3])
    #
    # @since 4.0.0
    def +(other_array)
      concatenate(other_array)
    end

    # Ensures returning the same data type array from the given array.
    #
    # @return [Arrow::Array]
    #
    # @overload resolve(other_raw_array)
    #
    #   @param other_raw_array [::Array] A raw Ruby Array. A new Arrow::Array
    #     is built by `self.class.new`.
    #
    #   @example Raw Ruby Array
    #     int32_array = Arrow::Int32Array.new([1])
    #     other_array = int32_array.resolve([2, 3, 4])
    #     other_array # => Arrow::Int32Array.new([2, 3, 4])
    #
    # @overload resolve(other_array)
    #
    #   @param other_array [Arrow::Array] Another Arrow::Array.
    #
    #     If the given other array is an same data type array of
    #     `self`, the given other array is returned as-is.
    #
    #     If the given other array isn't an same data type array of
    #     `self`, the given other array is casted.
    #
    #   @example Same data type
    #     int32_array = Arrow::Int32Array.new([1])
    #     other_int32_array = Arrow::Int32Array.new([2, 3, 4])
    #     other_array = int32_array.resolve(other_int32_array)
    #     other_array.object_id == other_int32_array.object_id
    #
    #   @example Other data type
    #     int32_array = Arrow::Int32Array.new([1])
    #     other_int8_array = Arrow::Int8Array.new([2, 3, 4])
    #     other_array = int32_array.resolve(other_int32_array)
    #     other_array #=> Arrow::Int32Array.new([2, 3, 4])
    #
    # @since 4.0.0
    def resolve(other_array)
      if other_array.is_a?(::Array)
        builder_class = self.class.builder_class
        if builder_class.nil?
          message =
            "[array][resolve] can't build #{value_data_type} array " +
            "from raw Ruby Array"
          raise ArgumentError, message
        end
        if builder_class.buildable?([other_array])
          other_array = builder_class.build(other_array)
        elsif builder_class.buildable?([value_data_type, other_array])
          other_array = builder_class.build(value_data_type, other_array)
        else
          message =
            "[array][resolve] need to implement " +
            "a feature that building #{value_data_type} array " +
            "from raw Ruby Array"
          raise NotImpelemented, message
        end
        other_array
      elsif other_array.respond_to?(:value_data_type)
        return other_array if value_data_type == other_array.value_data_type
        other_array.cast(value_data_type)
      else
        message =
          "[array][resolve] can't build #{value_data_type} array: " +
          "#{other_array.inspect}"
        raise ArgumentError, message
      end
    end
  end
end
