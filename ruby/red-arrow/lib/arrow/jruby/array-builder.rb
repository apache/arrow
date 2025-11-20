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

require_relative "array"

module Arrow
  module ArrayBuildable
    ValueVector = org.apache.arrow.vector.ValueVector
    def buildable?(args)
      return false if args.size == 1 and args.first.is_a?(ValueVector)
      super
    end
  end

  class ArrayBuilder
    class << self
      prepend ArrayBuildable
    end

    def initialize
      @vector = self.class::Array::Vector.new("", Arrow.allocator)
      @vector.allocate_new
      @index = 0
    end

    def append_value(value)
      @vector.set(@index, value)
      @index += 1
    end

    def append_values(values, is_valids=nil)
      if is_valids
        values.zip(is_valids) do |value, is_valid|
          if is_valid
            @vector.set(@index, value)
          else
            @vector.set_null(@index)
          end
          @index += 1
        end
      else
        values.each do |value|
          @vector.set(@index, value)
          @index += 1
        end
      end
    end

    def append_nulls(n)
      n.times do
        @vector.set_null(@index)
        @index += 1
      end
    end

    def finish
      @vector.set_value_count(@index)
      vector, @vector = @vector, nil
      self.class::Array.new(vector)
    end
  end

  class Int8ArrayBuilder < ArrayBuilder
    Array = Int8Array
  end

  class Int32ArrayBuilder < ArrayBuilder
    Array = Int32Array
  end

  class FixedSizeBinaryArrayBuilder < ArrayBuilder
  end

  class Decimal128ArrayBuilder < FixedSizeBinaryArrayBuilder
  end

  class Decimal256ArrayBuilder < FixedSizeBinaryArrayBuilder
  end

  class ListArrayBuilder < ArrayBuilder
  end

  class MapArrayBuilder < ArrayBuilder
  end

  class StructArrayBuilder < ArrayBuilder
  end

  class UnionArrayBuilder < ArrayBuilder
    def append_child(child, filed_name)
      raise NotImplementedError
    end
  end

  class DenseUnionArrayBuilder < UnionArrayBuilder
  end

  class SparseUnionArrayBuilder < UnionArrayBuilder
  end
end
