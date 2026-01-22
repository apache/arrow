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

module ArrowFormat
  class Type
  end

  class NullType < Type
    class << self
      def singleton
        @singleton ||= new
      end
    end

    def name
      "Null"
    end

    def build_array(size)
      NullArray.new(self, size)
    end

    def to_flatbuffers
      FB::Null::Data.new
    end
  end

  class PrimitiveType < Type
  end

  class BooleanType < PrimitiveType
    class << self
      def singleton
        @singleton ||= new
      end
    end

    def name
      "Boolean"
    end

    def build_array(size, validity_buffer, values_buffer)
      BooleanArray.new(self, size, validity_buffer, values_buffer)
    end

    def to_flatbuffers
      FB::Bool::Data.new
    end
  end

  class NumberType < PrimitiveType
  end

  class IntType < NumberType
    attr_reader :bit_width
    def initialize(bit_width, signed)
      super()
      @bit_width = bit_width
      @signed = signed
    end

    def signed?
      @signed
    end

    def to_flatbuffers
      fb_type = FB::Int::Data.new
      fb_type.bit_width = @bit_width
      fb_type.signed = @signed
      fb_type
    end
  end

  class Int8Type < IntType
    class << self
      def singleton
        @singleton ||= new
      end
    end

    def initialize
      super(8, true)
    end

    def name
      "Int8"
    end

    def buffer_type
      :S8
    end

    def build_array(size, validity_buffer, values_buffer)
      Int8Array.new(self, size, validity_buffer, values_buffer)
    end
  end

  class UInt8Type < IntType
    class << self
      def singleton
        @singleton ||= new
      end
    end

    def initialize
      super(8, false)
    end

    def name
      "UInt8"
    end

    def buffer_type
      :U8
    end

    def build_array(size, validity_buffer, values_buffer)
      UInt8Array.new(self, size, validity_buffer, values_buffer)
    end
  end

  class Int16Type < IntType
    class << self
      def singleton
        @singleton ||= new
      end
    end

    def initialize
      super(16, true)
    end

    def name
      "Int16"
    end

    def buffer_type
      :s16
    end

    def build_array(size, validity_buffer, values_buffer)
      Int16Array.new(self, size, validity_buffer, values_buffer)
    end
  end

  class UInt16Type < IntType
    class << self
      def singleton
        @singleton ||= new
      end
    end

    def initialize
      super(16, false)
    end

    def name
      "UInt16"
    end

    def buffer_type
      :u16
    end

    def build_array(size, validity_buffer, values_buffer)
      UInt16Array.new(self, size, validity_buffer, values_buffer)
    end
  end

  class Int32Type < IntType
    class << self
      def singleton
        @singleton ||= new
      end
    end

    def initialize
      super(32, true)
    end

    def name
      "Int32"
    end

    def buffer_type
      :s32
    end

    def build_array(size, validity_buffer, values_buffer)
      Int32Array.new(self, size, validity_buffer, values_buffer)
    end
  end

  class UInt32Type < IntType
    class << self
      def singleton
        @singleton ||= new
      end
    end

    def initialize
      super(32, false)
    end

    def name
      "UInt32"
    end

    def buffer_type
      :u32
    end

    def build_array(size, validity_buffer, values_buffer)
      UInt32Array.new(self, size, validity_buffer, values_buffer)
    end
  end

  class Int64Type < IntType
    class << self
      def singleton
        @singleton ||= new
      end
    end

    def initialize
      super(64, true)
    end

    def name
      "Int64"
    end

    def buffer_type
      :s64
    end

    def build_array(size, validity_buffer, values_buffer)
      Int64Array.new(self, size, validity_buffer, values_buffer)
    end
  end

  class UInt64Type < IntType
    class << self
      def singleton
        @singleton ||= new
      end
    end

    def initialize
      super(64, false)
    end

    def name
      "UInt64"
    end

    def buffer_type
      :u64
    end

    def build_array(size, validity_buffer, values_buffer)
      UInt64Array.new(self, size, validity_buffer, values_buffer)
    end
  end

  class FloatingPointType < NumberType
    attr_reader :precision
    def initialize(precision)
      super()
      @precision = precision
    end
  end

  class Float32Type < FloatingPointType
    class << self
      def singleton
        @singleton ||= new
      end
    end

    def initialize
      super(:single)
    end

    def name
      "Float32"
    end

    def build_array(size, validity_buffer, values_buffer)
      Float32Array.new(self, size, validity_buffer, values_buffer)
    end
  end

  class Float64Type < FloatingPointType
    class << self
      def singleton
        @singleton ||= new
      end
    end

    def initialize
      super(:double)
    end

    def name
      "Float64"
    end

    def build_array(size, validity_buffer, values_buffer)
      Float64Array.new(self, size, validity_buffer, values_buffer)
    end
  end

  class TemporalType < Type
  end

  class DateType < TemporalType
  end

  class Date32Type < DateType
    class << self
      def singleton
        @singleton ||= new
      end
    end

    def name
      "Date32"
    end

    def build_array(size, validity_buffer, values_buffer)
      Date32Array.new(self, size, validity_buffer, values_buffer)
    end
  end

  class Date64Type < DateType
    class << self
      def singleton
        @singleton ||= new
      end
    end

    def name
      "Date64"
    end

    def build_array(size, validity_buffer, values_buffer)
      Date64Array.new(self, size, validity_buffer, values_buffer)
    end
  end

  class TimeType < TemporalType
    attr_reader :unit
    def initialize(unit)
      super()
      @unit = unit
    end
  end

  class Time32Type < TimeType
    def name
      "Time32"
    end

    def build_array(size, validity_buffer, values_buffer)
      Time32Array.new(self, size, validity_buffer, values_buffer)
    end
  end

  class Time64Type < TimeType
    def name
      "Time64"
    end

    def build_array(size, validity_buffer, values_buffer)
      Time64Array.new(self, size, validity_buffer, values_buffer)
    end
  end

  class TimestampType < TemporalType
    attr_reader :unit
    attr_reader :timezone
    def initialize(unit, timezone)
      super()
      @unit = unit
      @timezone = timezone
    end

    def name
      "Timestamp"
    end

    def build_array(size, validity_buffer, values_buffer)
      TimestampArray.new(self, size, validity_buffer, values_buffer)
    end
  end

  class IntervalType < TemporalType
  end

  class YearMonthIntervalType < IntervalType
    def name
      "YearMonthInterval"
    end

    def build_array(size, validity_buffer, values_buffer)
      YearMonthIntervalArray.new(self, size, validity_buffer, values_buffer)
    end
  end

  class DayTimeIntervalType < IntervalType
    def name
      "DayTimeInterval"
    end

    def build_array(size, validity_buffer, values_buffer)
      DayTimeIntervalArray.new(self, size, validity_buffer, values_buffer)
    end
  end

  class MonthDayNanoIntervalType < IntervalType
    def name
      "MonthDayNanoInterval"
    end

    def build_array(size, validity_buffer, values_buffer)
      MonthDayNanoIntervalArray.new(self,
                                    size,
                                    validity_buffer,
                                    values_buffer)
    end
  end

  class DurationType < TemporalType
    attr_reader :unit
    def initialize(unit)
      super()
      @unit = unit
    end

    def name
      "Duration"
    end

    def build_array(size, validity_buffer, values_buffer)
      DurationArray.new(self, size, validity_buffer, values_buffer)
    end
  end

  class VariableSizeBinaryType < Type
  end

  class BinaryType < VariableSizeBinaryType
    class << self
      def singleton
        @singleton ||= new
      end
    end

    def name
      "Binary"
    end

    def build_array(size, validity_buffer, offsets_buffer, values_buffer)
      BinaryArray.new(self,
                      size,
                      validity_buffer,
                      offsets_buffer,
                      values_buffer)
    end

    def to_flatbuffers
      FB::Binary::Data.new
    end
  end

  class LargeBinaryType < VariableSizeBinaryType
    class << self
      def singleton
        @singleton ||= new
      end
    end

    def name
      "LargeBinary"
    end

    def build_array(size, validity_buffer, offsets_buffer, values_buffer)
      LargeBinaryArray.new(self,
                           size,
                           validity_buffer,
                           offsets_buffer,
                           values_buffer)
    end
  end

  class UTF8Type < VariableSizeBinaryType
    class << self
      def singleton
        @singleton ||= new
      end
    end

    def name
      "UTF8"
    end

    def build_array(size, validity_buffer, offsets_buffer, values_buffer)
      UTF8Array.new(self, size, validity_buffer, offsets_buffer, values_buffer)
    end
  end

  class LargeUTF8Type < VariableSizeBinaryType
    class << self
      def singleton
        @singleton ||= new
      end
    end

    def name
      "LargeUTF8"
    end

    def build_array(size, validity_buffer, offsets_buffer, values_buffer)
      LargeUTF8Array.new(self,
                         size,
                         validity_buffer,
                         offsets_buffer,
                         values_buffer)
    end
  end

  class FixedSizeBinaryType < Type
    attr_reader :byte_width
    def initialize(byte_width)
      super()
      @byte_width = byte_width
    end

    def name
      "FixedSizeBinary"
    end

    def build_array(size, validity_buffer, values_buffer)
      FixedSizeBinaryArray.new(self, size, validity_buffer, values_buffer)
    end
  end

  class DecimalType < FixedSizeBinaryType
    attr_reader :precision
    attr_reader :scale
    def initialize(byte_width, precision, scale)
      super(byte_width)
      @precision = precision
      @scale = scale
    end
  end

  class Decimal128Type < DecimalType
    def initialize(precision, scale)
      super(16, precision, scale)
    end

    def name
      "Decimal128"
    end

    def build_array(size, validity_buffer, values_buffer)
      Decimal128Array.new(self, size, validity_buffer, values_buffer)
    end
  end

  class Decimal256Type < DecimalType
    def initialize(precision, scale)
      super(32, precision, scale)
    end

    def name
      "Decimal256"
    end

    def build_array(size, validity_buffer, values_buffer)
      Decimal256Array.new(self, size, validity_buffer, values_buffer)
    end
  end

  class VariableSizeListType < Type
    attr_reader :child
    def initialize(child)
      super()
      @child = child
    end

  end

  class ListType < VariableSizeListType
    def name
      "List"
    end

    def build_array(size, validity_buffer, offsets_buffer, child)
      ListArray.new(self, size, validity_buffer, offsets_buffer, child)
    end
  end

  class LargeListType < VariableSizeListType
    def name
      "LargeList"
    end

    def build_array(size, validity_buffer, offsets_buffer, child)
      LargeListArray.new(self, size, validity_buffer, offsets_buffer, child)
    end
  end

  class StructType < Type
    attr_reader :children
    def initialize(children)
      super()
      @children = children
    end

    def name
      "Struct"
    end

    def build_array(size, validity_buffer, children)
      StructArray.new(self, size, validity_buffer, children)
    end
  end

  class MapType < VariableSizeListType
    def initialize(child)
      if child.nullable?
        raise TypeError.new("Map entry field must not be nullable: " +
                            child.inspect)
      end
      type = child.type
      unless type.is_a?(StructType)
        raise TypeError.new("Map entry type must be struct: #{type.inspect}")
      end
      unless type.children.size == 2
        raise TypeError.new("Map entry struct type must have 2 children: " +
                            type.inspect)
      end
      if type.children[0].nullable?
        raise TypeError.new("Map key field must not be nullable: " +
                            type.children[0].inspect)
      end
      super(child)
    end

    def name
      "Map"
    end

    def build_array(size, validity_buffer, offsets_buffer, child)
      MapArray.new(self, size, validity_buffer, offsets_buffer, child)
    end
  end

  class UnionType < Type
    attr_reader :children
    attr_reader :type_ids
    def initialize(children, type_ids)
      super()
      @children = children
      @type_ids = type_ids
      @type_indexes = {}
    end

    def resolve_type_index(type)
      @type_indexes[type] ||= @type_ids.index(type)
    end
  end

  class DenseUnionType < UnionType
    def name
      "DenseUnion"
    end

    def build_array(size, types_buffer, offsets_buffer, children)
      DenseUnionArray.new(self, size, types_buffer, offsets_buffer, children)
    end
  end

  class SparseUnionType < UnionType
    def name
      "SparseUnion"
    end

    def build_array(size, types_buffer, children)
      SparseUnionArray.new(self, size, types_buffer, children)
    end
  end

  class DictionaryType < Type
    attr_reader :index_type
    attr_reader :value_type
    def initialize(index_type, value_type, ordered)
      super()
      @index_type = index_type
      @value_type = value_type
      @ordered = ordered
    end

    def ordered?
      @ordered
    end

    def name
      "Dictionary"
    end

    def build_array(size, validity_buffer, indices_buffer, dictionary)
      DictionaryArray.new(self,
                          size,
                          validity_buffer,
                          indices_buffer,
                          dictionary)
    end
  end
end
