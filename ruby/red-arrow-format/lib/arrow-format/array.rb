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

require "bigdecimal"

require_relative "bitmap"

module ArrowFormat
  class Array
    attr_reader :type
    attr_reader :size
    alias_method :length, :size
    attr_reader :offset
    attr_reader :validity_buffer
    def initialize(type, size, validity_buffer)
      @type = type
      @size = size
      @offset = 0
      @validity_buffer = validity_buffer
      @sliced_buffers = {}
    end

    def slice(offset, size=nil)
      sliced = dup
      sliced.slice!(@offset + offset, size || @size - offset)
      sliced
    end

    def valid?(i)
      return true if @validity_buffer.nil?
      validity_bitmap[i]
    end

    def null?(i)
      not valid?(i)
    end

    def n_nulls
      if @validity_buffer.nil?
        0
      else
        @size - validity_bitmap.popcount
      end
    end

    protected
    def slice!(offset, size)
      @offset = offset
      @size = size
      clear_cache
    end

    private
    def validity_bitmap
      @validity_bitmap ||= Bitmap.new(@validity_buffer, @offset, @size)
    end

    def apply_validity(array)
      return array if @validity_buffer.nil?
      validity_bitmap.each_with_index do |is_valid, i|
        array[i] = nil unless is_valid
      end
      array
    end

    def clear_cache
      @validity_bitmap = nil
      @sliced_buffers = {}
    end

    def slice_buffer(id, buffer)
      return buffer if buffer.nil?
      return buffer if @offset.zero?

      @sliced_buffers[id] ||= yield(buffer)
    end

    def slice_bitmap_buffer(id, buffer)
      slice_buffer(id, buffer) do
        if (@offset % 8).zero?
          buffer.slice(@offset / 8)
        else
          # We need to copy because we can't do bit level slice.
          # TODO: Optimize.
          valid_bytes = []
          Bitmap.new(buffer, @offset, @size).each_slice(8) do |valids|
            valid_byte = 0
            valids.each_with_index do |valid, i|
              valid_byte |= 1 << (i % 8) if valid
            end
            valid_bytes << valid_byte
          end
          IO::Buffer.for(valid_bytes.pack("C*"))
        end
      end
    end

    def slice_fixed_element_size_buffer(id, buffer, element_size)
      slice_buffer(id, buffer) do
        buffer.slice(element_size * @offset)
      end
    end

    def slice_offsets_buffer(id, buffer, buffer_type)
      slice_buffer(id, buffer) do
        offset_size = IO::Buffer.size_of(buffer_type)
        buffer_offset = offset_size * (@offset - 1)
        first_offset = buffer.get_value(buffer_type, buffer_offset)
        # TODO: Optimize
        sliced_buffer = IO::Buffer.new(offset_size * (@size + 1))
        buffer.each(buffer_type,
                    buffer_offset,
                    @size + 1).with_index do |(_, offset), i|
          new_offset = offset - first_offset
          sliced_buffer.set_value(buffer_type,
                                  offset_size * i,
                                  new_offset)
        end
        sliced_buffer
      end
    end
  end

  class NullArray < Array
    def initialize(type, size)
      super(type, size, nil)
    end

    def each_buffer
      return to_enum(__method__) unless block_given?
    end

    def to_a
      [nil] * @size
    end
  end

  class PrimitiveArray < Array
    def initialize(type, size, validity_buffer, values_buffer)
      super(type, size, validity_buffer)
      @values_buffer = values_buffer
    end

    def to_a
      offset = element_size * @offset
      apply_validity(@values_buffer.values(@type.buffer_type, offset, @size))
    end

    def each_buffer
      return to_enum(__method__) unless block_given?

      yield(slice_bitmap_buffer(:validity, @validity_buffer))
      yield(slice_fixed_element_size_buffer(:values,
                                            @values_buffer,
                                            element_size))
    end

    private
    def element_size
      IO::Buffer.size_of(@type.buffer_type)
    end
  end

  class BooleanArray < PrimitiveArray
    def to_a
      @values_bitmap ||= Bitmap.new(@values_buffer, @offset, @size)
      values = @values_bitmap.to_a
      apply_validity(values)
    end

    def each_buffer
      return to_enum(__method__) unless block_given?

      yield(slice_bitmap_buffer(:validity, @validity_buffer))
      yield(slice_bitmap_buffer(:values, @values_buffer))
    end

    private
    def clear_cache
      super
      @values_bitmap = nil
    end
  end

  class IntArray < PrimitiveArray
  end

  class Int8Array < IntArray
  end

  class UInt8Array < IntArray
  end

  class Int16Array < IntArray
  end

  class UInt16Array < IntArray
  end

  class Int32Array < IntArray
  end

  class UInt32Array < IntArray
  end

  class Int64Array < IntArray
  end

  class UInt64Array < IntArray
  end

  class FloatingPointArray < PrimitiveArray
  end

  class Float32Array < FloatingPointArray
  end

  class Float64Array < FloatingPointArray
  end

  class TemporalArray < PrimitiveArray
  end

  class DateArray < TemporalArray
  end

  class Date32Array < DateArray
  end

  class Date64Array < DateArray
  end

  class TimeArray < TemporalArray
  end

  class Time32Array < TimeArray
  end

  class Time64Array < TimeArray
  end

  class TimestampArray < TemporalArray
  end

  class IntervalArray < TemporalArray
  end

  class YearMonthIntervalArray < IntervalArray
  end

  class DayTimeIntervalArray < IntervalArray
    def to_a
      offset = element_size * @offset
      values = @values_buffer.
                 each(@type.buffer_type, offset, @size * 2).
                 each_slice(2).
                 collect do |(_, day), (_, time)|
        [day, time]
      end
      apply_validity(values)
    end
  end

  class MonthDayNanoIntervalArray < IntervalArray
    def to_a
      buffer_types = @type.buffer_types
      value_size = IO::Buffer.size_of(buffer_types)
      base_offset = value_size * @offset
      values = @size.times.collect do |i|
        offset = base_offset + value_size * i
        @values_buffer.get_values(buffer_types, offset)
      end
      apply_validity(values)
    end

    private
    def element_size
      IO::Buffer.size_of(@type.buffer_types)
    end
  end

  class DurationArray < TemporalArray
  end

  class VariableSizeBinaryLayoutArray < Array
    def initialize(type, size, validity_buffer, offsets_buffer, values_buffer)
      super(type, size, validity_buffer)
      @offsets_buffer = offsets_buffer
      @values_buffer = values_buffer
    end

    def each_buffer
      return to_enum(__method__) unless block_given?

      yield(slice_bitmap_buffer(:validity, @validity_buffer))
      yield(slice_offsets_buffer(:offsets,
                                 @offsets_buffer,
                                 @type.offset_buffer_type))
      sliced_values_buffer = slice_buffer(:values, @values_buffer) do
        first_offset = @offsets_buffer.get_value(@type.offset_buffer_type,
                                                 offset_size * @offset)
        @values_buffer.slice(first_offset)
      end
      yield(sliced_values_buffer)
    end

    def to_a
      values = @offsets_buffer.
        each(@type.offset_buffer_type, offset_size * @offset, @size + 1).
        each_cons(2).
        collect do |(_, offset), (_, next_offset)|
        length = next_offset - offset
        @values_buffer.get_string(offset, length, @type.encoding)
      end
      apply_validity(values)
    end

    private
    def offset_size
      IO::Buffer.size_of(@type.offset_buffer_type)
    end
  end

  class BinaryArray < VariableSizeBinaryLayoutArray
  end

  class LargeBinaryArray < VariableSizeBinaryLayoutArray
  end

  class UTF8Array < VariableSizeBinaryLayoutArray
  end

  class LargeUTF8Array < VariableSizeBinaryLayoutArray
  end

  class FixedSizeBinaryArray < Array
    def initialize(type, size, validity_buffer, values_buffer)
      super(type, size, validity_buffer)
      @values_buffer = values_buffer
    end

    def each_buffer
      return to_enum(__method__) unless block_given?

      yield(slice_bitmap_buffer(:validity, @validity_buffer))
      yield(slice_fixed_element_size_buffer(:values,
                                            @values_buffer,
                                            @type.byte_width))
    end

    def to_a
      byte_width = @type.byte_width
      values = 0.step(@size * byte_width - 1, byte_width).collect do |offset|
        @values_buffer.get_string(offset, byte_width)
      end
      apply_validity(values)
    end
  end

  class DecimalArray < FixedSizeBinaryArray
    def to_a
      byte_width = @type.byte_width
      buffer_types = [:u64] * (byte_width / 8 - 1) + [:s64]
      base_offset = byte_width * @offset
      values = 0.step(@size * byte_width - 1, byte_width).collect do |offset|
        @values_buffer.get_values(buffer_types, base_offset + offset)
      end
      apply_validity(values).collect do |value|
        if value.nil?
          nil
        else
          BigDecimal(format_value(value))
        end
      end
    end

    private
    def format_value(components)
      highest = components.last
      width = @type.precision
      width += 1 if highest < 0
      value = 0
      components.reverse_each do |component|
        value = (value << 64) + component
      end
      string = value.to_s
      if @type.scale < 0
        string << ("0" * -@type.scale)
      elsif @type.scale > 0
        n_digits = string.bytesize
        n_digits -= 1 if value < 0
        if n_digits < @type.scale
          prefix = "0." + ("0" * (@type.scale - n_digits - 1))
          if value < 0
            string[1, 0] = prefix
          else
            string[0, 0] = prefix
          end
        else
          string[-@type.scale, 0] = "."
        end
      end
      string
    end
  end

  class Decimal128Array < DecimalArray
  end

  class Decimal256Array < DecimalArray
  end

  class VariableSizeListArray < Array
    attr_reader :child
    def initialize(type, size, validity_buffer, offsets_buffer, child)
      super(type, size, validity_buffer)
      @offsets_buffer = offsets_buffer
      @child = child
    end

    def each_buffer(&block)
      return to_enum(__method__) unless block_given?

      yield(slice_bitmap_buffer(:validity, @validity_buffer))
      yield(slice_offsets_buffer(:offsets,
                                 @offsets_buffer,
                                 @type.offset_buffer_type))
    end

    def to_a
      child_values = @child.to_a
      values = @offsets_buffer.
        each(@type.offset_buffer_type, offset_size * @offset, @size + 1).
        each_cons(2).
        collect do |(_, offset), (_, next_offset)|
        child_values[offset...next_offset]
      end
      apply_validity(values)
    end

    private
    def offset_size
      IO::Buffer.size_of(@type.offset_buffer_type)
    end

    def slice!(offset, size)
      super
      first_offset =
        @offsets_buffer.get_value(@type.offset_buffer_type,
                                  offset_size * @offset)
      last_offset =
        @offsets_buffer.get_value(@type.offset_buffer_type,
                                  offset_size * (@offset + @size + 1))
      @child = @child.slice(first_offset, last_offset - first_offset)
    end
  end

  class ListArray < VariableSizeListArray
  end

  class LargeListArray < VariableSizeListArray
  end

  class StructArray < Array
    attr_reader :children
    def initialize(type, size, validity_buffer, children)
      super(type, size, validity_buffer)
      @children = children
    end

    def each_buffer(&block)
      return to_enum(__method__) unless block_given?

      yield(slice_bitmap_buffer(:validity, @validity_buffer))
    end

    def to_a
      if @children.empty?
        values = [[]] * @size
      else
        children_values = @children.collect(&:to_a)
        values = children_values[0].zip(*children_values[1..-1])
      end
      apply_validity(values)
    end

    private
    def slice!(offset, size)
      super
      @children = @children.collect do |child|
        child.slice(offset, size)
      end
    end
  end

  class MapArray < VariableSizeListArray
    def to_a
      super.collect do |entries|
        if entries.nil?
          entries
        else
          hash = {}
          entries.each do |key, value|
            hash[key] = value
          end
          hash
        end
      end
    end
  end

  class UnionArray < Array
    attr_reader :children
    def initialize(type, size, types_buffer, children)
      super(type, size, nil)
      @types_buffer = types_buffer
      @children = children
    end

    private
    def type_buffer_type
      :S8
    end

    def type_element_size
      IO::Buffer.size_of(type_buffer_type)
    end
  end

  class DenseUnionArray < UnionArray
    def initialize(type,
                   size,
                   types_buffer,
                   offsets_buffer,
                   children)
      super(type, size, types_buffer, children)
      @offsets_buffer = offsets_buffer
    end

    def each_buffer(&block)
      return to_enum(__method__) unless block_given?

      # TODO: Dictionary delta support (slice support)
      yield(@types_buffer)
      yield(@offsets_buffer)
    end

    def to_a
      children_values = @children.collect(&:to_a)
      types = @types_buffer.each(type_buffer_type,
                                 type_element_size * @offset,
                                 @size)
      offsets = @offsets_buffer.each(:s32,
                                     offset_element_size * @offset,
                                     @size)
      types.zip(offsets).collect do |(_, type), (_, offset)|
        index = @type.resolve_type_index(type)
        children_values[index][offset]
      end
    end

    private
    def offset_buffer_type
      :s32
    end

    def offset_element_size
      IO::Buffer.size_of(offset_buffer_type)
    end
  end

  class SparseUnionArray < UnionArray
    def each_buffer(&block)
      return to_enum(__method__) unless block_given?

      yield(slice_fixed_element_size_buffer(:types,
                                            @types_buffer,
                                            type_element_size))
    end

    def to_a
      children_values = @children.collect(&:to_a)
      @types_buffer.each(type_buffer_type,
                         type_element_size * @offset,
                         @size).with_index.collect do |(_, type), i|
        index = @type.resolve_type_index(type)
        children_values[index][i]
      end
    end

    private
    def slice!(offset, size)
      super
      @children = @children.collect do |child|
        child.slice(offset, size)
      end
    end
  end

  class DictionaryArray < Array
    attr_reader :indices_buffer
    attr_reader :dictionary
    def initialize(type, size, validity_buffer, indices_buffer, dictionary)
      super(type, size, validity_buffer)
      @indices_buffer = indices_buffer
      @dictionary = dictionary
    end

    # TODO: Slice support
    def each_buffer
      return to_enum(__method__) unless block_given?

      yield(@validity_buffer)
      yield(@indices_buffer)
    end

    def to_a
      values = []
      @dictionary.each do |dictionary_chunk|
        values.concat(dictionary_chunk.to_a)
      end
      buffer_type = @type.index_type.buffer_type
      offset = IO::Buffer.size_of(buffer_type) * @offset
      indices =
        apply_validity(@indices_buffer.values(buffer_type, offset, @size))
      indices.collect do |index|
        if index.nil?
          nil
        else
          values[index]
        end
      end
    end
  end
end
