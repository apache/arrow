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
    attr_reader :name
    def initialize(name)
      @name = name
    end
  end

  class NullType < Type
    class << self
      def singleton
        @singleton ||= new
      end
    end

    def initialize
      super("Null")
    end

    def build_array(size)
      NullArray.new(self, size)
    end
  end

  class BooleanType < Type
    class << self
      def singleton
        @singleton ||= new
      end
    end

    def initialize
      super("Boolean")
    end

    def build_array(size, validity_buffer, values_buffer)
      BooleanArray.new(self, size, validity_buffer, values_buffer)
    end
  end

  class NumberType < Type
  end

  class IntType < NumberType
    attr_reader :bit_width
    attr_reader :signed
    def initialize(name, bit_width, signed)
      super(name)
      @bit_width = bit_width
      @signed = signed
    end
  end

  class Int8Type < IntType
    class << self
      def singleton
        @singleton ||= new
      end
    end

    def initialize
      super("Int8", 8, true)
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
      super("UInt8", 8, false)
    end

    def build_array(size, validity_buffer, values_buffer)
      UInt8Array.new(self, size, validity_buffer, values_buffer)
    end
  end

  class FloatingPointType < NumberType
    attr_reader :precision
    def initialize(name, precision)
      super(name)
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
      super("Float32", :single)
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
      super("Float64", :double)
    end

    def build_array(size, validity_buffer, values_buffer)
      Float64Array.new(self, size, validity_buffer, values_buffer)
    end
  end

  class DateType < Type
  end

  class Date32Type < DateType
    class << self
      def singleton
        @singleton ||= new
      end
    end

    def initialize
      super("Date32")
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

    def initialize
      super("Date64")
    end

    def build_array(size, validity_buffer, values_buffer)
      Date64Array.new(self, size, validity_buffer, values_buffer)
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

    def initialize
      super("Binary")
    end

    def build_array(size, validity_buffer, offsets_buffer, values_buffer)
      BinaryArray.new(self, size, validity_buffer, offsets_buffer, values_buffer)
    end
  end

  class LargeBinaryType < VariableSizeBinaryType
    class << self
      def singleton
        @singleton ||= new
      end
    end

    def initialize
      super("LargeBinary")
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

    attr_reader :name
    def initialize
      super("UTF8")
    end

    def build_array(size, validity_buffer, offsets_buffer, values_buffer)
      UTF8Array.new(self, size, validity_buffer, offsets_buffer, values_buffer)
    end
  end

  class VariableSizeListType < Type
    attr_reader :child
    def initialize(name, child)
      super(name)
      @child = child
    end

  end

  class ListType < VariableSizeListType
    def initialize(child)
      super("List", child)
    end

    def build_array(size, validity_buffer, offsets_buffer, child)
      ListArray.new(self, size, validity_buffer, offsets_buffer, child)
    end
  end

  class LargeListType < VariableSizeListType
    def initialize(child)
      super("LargeList", child)
    end

    def build_array(size, validity_buffer, offsets_buffer, child)
      LargeListArray.new(self, size, validity_buffer, offsets_buffer, child)
    end
  end

  class StructType < Type
    attr_reader :children
    def initialize(children)
      super("Struct")
      @children = children
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
      super("Map", child)
    end

    def build_array(size, validity_buffer, offsets_buffer, child)
      MapArray.new(self, size, validity_buffer, offsets_buffer, child)
    end
  end

  class UnionType < Type
    attr_reader :children
    attr_reader :type_ids
    def initialize(name, children, type_ids)
      super(name)
      @children = children
      @type_ids = type_ids
      @type_indexes = {}
    end

    def resolve_type_index(type)
      @type_indexes[type] ||= @type_ids.index(type)
    end
  end

  class DenseUnionType < UnionType
    def initialize(children, type_ids)
      super("DenseUnion", children, type_ids)
    end

    def build_array(size, types_buffer, offsets_buffer, children)
      DenseUnionArray.new(self, size, types_buffer, offsets_buffer, children)
    end
  end

  class SparseUnionType < UnionType
    def initialize(children, type_ids)
      super("SparseUnion", children, type_ids)
    end

    def build_array(size, types_buffer, children)
      SparseUnionArray.new(self, size, types_buffer, children)
    end
  end
end
