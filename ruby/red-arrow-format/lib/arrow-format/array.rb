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

require_relative "bitmap"

module ArrowFormat
  class Array
    attr_reader :type
    attr_reader :size
    alias_method :length, :size
    def initialize(type, size, validity_buffer)
      @type = type
      @size = size
      @validity_buffer = validity_buffer
    end

    def valid?(i)
      return true if @validity_buffer.nil?
      (@validity_buffer.get_value(:U8, i / 8) & (1 << (i % 8))) > 0
    end

    def null?(i)
      not valid?(i)
    end

    private
    def apply_validity(array)
      return array if @validity_buffer.nil?
      @validity_bitmap ||= Bitmap.new(@validity_buffer, @size)
      @validity_bitmap.each_with_index do |bit, i|
        array[i] = nil if bit.zero?
      end
      array
    end
  end

  class NullArray < Array
    def initialize(type, size)
      super(type, size, nil)
    end

    def to_a
      [nil] * @size
    end
  end

  class BooleanArray < Array
    def initialize(type, size, validity_buffer, values_buffer)
      super(type, size, validity_buffer)
      @values_buffer = values_buffer
    end

    def to_a
      @values_bitmap ||= Bitmap.new(@values_buffer, @size)
      values = @values_bitmap.each.collect do |bit|
        not bit.zero?
      end
      apply_validity(values)
    end
  end

  class IntArray < Array
    def initialize(type, size, validity_buffer, values_buffer)
      super(type, size, validity_buffer)
      @values_buffer = values_buffer
    end
  end

  class Int8Array < IntArray
    def to_a
      apply_validity(@values_buffer.values(:S8, 0, @size))
    end
  end

  class UInt8Array < IntArray
    def to_a
      apply_validity(@values_buffer.values(:U8, 0, @size))
    end
  end

  class Int16Array < IntArray
    def to_a
      apply_validity(@values_buffer.values(:s16, 0, @size))
    end
  end

  class UInt16Array < IntArray
    def to_a
      apply_validity(@values_buffer.values(:u16, 0, @size))
    end
  end

  class Int32Array < IntArray
    def to_a
      apply_validity(@values_buffer.values(:s32, 0, @size))
    end
  end

  class UInt32Array < IntArray
    def to_a
      apply_validity(@values_buffer.values(:u32, 0, @size))
    end
  end

  class Int64Array < IntArray
    def to_a
      apply_validity(@values_buffer.values(:s64, 0, @size))
    end
  end

  class UInt64Array < IntArray
    def to_a
      apply_validity(@values_buffer.values(:u64, 0, @size))
    end
  end

  class FloatingPointArray < Array
    def initialize(type, size, validity_buffer, values_buffer)
      super(type, size, validity_buffer)
      @values_buffer = values_buffer
    end
  end

  class Float32Array < FloatingPointArray
    def to_a
      apply_validity(@values_buffer.values(:f32, 0, @size))
    end
  end

  class Float64Array < FloatingPointArray
    def to_a
      apply_validity(@values_buffer.values(:f64, 0, @size))
    end
  end

  class TemporalArray < Array
    def initialize(type, size, validity_buffer, values_buffer)
      super(type, size, validity_buffer)
      @values_buffer = values_buffer
    end
  end

  class DateArray < TemporalArray
  end

  class Date32Array < DateArray
    def to_a
      apply_validity(@values_buffer.values(:s32, 0, @size))
    end
  end

  class Date64Array < DateArray
    def to_a
      apply_validity(@values_buffer.values(:s64, 0, @size))
    end
  end

  class TimeArray < TemporalArray
  end

  class Time32Array < TimeArray
    def to_a
      apply_validity(@values_buffer.values(:s32, 0, @size))
    end
  end

  class Time64Array < TimeArray
    def to_a
      apply_validity(@values_buffer.values(:s64, 0, @size))
    end
  end

  class VariableSizeBinaryLayoutArray < Array
    def initialize(type, size, validity_buffer, offsets_buffer, values_buffer)
      super(type, size, validity_buffer)
      @offsets_buffer = offsets_buffer
      @values_buffer = values_buffer
    end

    def to_a
      values = @offsets_buffer.
        each(buffer_type, 0, @size + 1).
        each_cons(2).
        collect do |(_, offset), (_, next_offset)|
        length = next_offset - offset
        @values_buffer.get_string(offset, length, encoding)
      end
      apply_validity(values)
    end
  end

  class BinaryArray < VariableSizeBinaryLayoutArray
    private
    def buffer_type
      :s32 # TODO: big endian support
    end

    def encoding
      Encoding::ASCII_8BIT
    end
  end

  class LargeBinaryArray < VariableSizeBinaryLayoutArray
    private
    def buffer_type
      :s64 # TODO: big endian support
    end

    def encoding
      Encoding::ASCII_8BIT
    end
  end

  class UTF8Array < VariableSizeBinaryLayoutArray
    private
    def buffer_type
      :s32 # TODO: big endian support
    end

    def encoding
      Encoding::UTF_8
    end
  end

  class VariableSizeListArray < Array
    def initialize(type, size, validity_buffer, offsets_buffer, child)
      super(type, size, validity_buffer)
      @offsets_buffer = offsets_buffer
      @child = child
    end

    def to_a
      child_values = @child.to_a
      values = @offsets_buffer.
        each(offset_type, 0, @size + 1).
        each_cons(2).
        collect do |(_, offset), (_, next_offset)|
        child_values[offset...next_offset]
      end
      apply_validity(values)
    end
  end

  class ListArray < VariableSizeListArray
    private
    def offset_type
      :s32 # TODO: big endian support
    end
  end

  class LargeListArray < VariableSizeListArray
    private
    def offset_type
      :s64 # TODO: big endian support
    end
  end

  class StructArray < Array
    def initialize(type, size, validity_buffer, children)
      super(type, size, validity_buffer)
      @children = children
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
  end

  class UnionArray < Array
    def initialize(type, size, types_buffer, children)
      super(type, size, nil)
      @types_buffer = types_buffer
      @children = children
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

    def to_a
      children_values = @children.collect(&:to_a)
      types = @types_buffer.each(:S8, 0, @size)
      offsets = @offsets_buffer.each(:s32, 0, @size)
      types.zip(offsets).collect do |(_, type), (_, offset)|
        index = @type.resolve_type_index(type)
        children_values[index][offset]
      end
    end
  end

  class SparseUnionArray < UnionArray
    def to_a
      children_values = @children.collect(&:to_a)
      @types_buffer.each(:S8, 0, @size).with_index.collect do |(_, type), i|
        index = @type.resolve_type_index(type)
        children_values[index][i]
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

    private
    def offset_type
      :s32 # TODO: big endian support
    end
  end
end
