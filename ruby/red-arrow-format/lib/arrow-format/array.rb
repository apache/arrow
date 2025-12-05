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

  class VariableSizeBinaryLayoutArray < Array
    def initialize(type, size, validity_buffer, offsets_buffer, values_buffer)
      super(type, size, validity_buffer)
      @offsets_buffer = offsets_buffer
      @values_buffer = values_buffer
    end

    def to_a
      values = @offsets_buffer.
        each(:s32, 0, @size + 1). # TODO: big endian support
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
    def encoding
      Encoding::ASCII_8BIT
    end
  end

  class UTF8Array < VariableSizeBinaryLayoutArray
    private
    def encoding
      Encoding::UTF_8
    end
  end

  class ListArray < Array
    def initialize(type, size, validity_buffer, offsets_buffer, child)
      super(type, size, validity_buffer)
      @offsets_buffer = offsets_buffer
      @child = child
    end

    def to_a
      child_values = @child.to_a
      values = @offsets_buffer.
        each(:s32, 0, @size + 1). # TODO: big endian support
        each_cons(2).
        collect do |(_, offset), (_, next_offset)|
        child_values[offset...next_offset]
      end
      apply_validity(values)
    end
  end
end
