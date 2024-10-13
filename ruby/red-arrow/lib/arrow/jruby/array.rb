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

require_relative "data-type"

module Arrow
  class Array
    VectorAppender = org.apache.arrow.vector.util.VectorAppender
    VectorEqualsVisitor = org.apache.arrow.vector.compare.VectorEqualsVisitor

    attr_reader :vector

    def initialize(vector)
      @vector = vector
    end

    def ==(other_array)
      return false unless other_array.is_a?(self.class)
      VectorEqualsVisitor.vector_equals(@vector, other_array.vector)
    end

    def null?(i)
      @vector.null?(i)
    end

    def get_value(i)
      @vector.get_object(i)
    end

    def to_s
      @vector.to_s
    end

    def inspect
      super.sub(/>\z/) do
        " #{to_s}>"
      end
    end

    def close
      @vector.close
    end

    def length
      @vector.value_count
    end

    def value_data_type
      self.class::ValueDataType.new
    end

    def values
      each.to_a
    end

    def cast(other_value_data_type)
      other_value_data_type.build_array(to_a)
    end

    def is_in(values)
      raise NotImplementedError
    end

    def concatenate(other_arrays)
      total_size = length + other_arrays.sum(&:length)
      vector = self.class::Vector.new("", Arrow.allocator)
      vector.allocate_new(total_size)
      appender = VectorAppender.new(vector)
      @vector.accept(appender, nil)
      other_arrays.each do |other_array|
        other_array.vector.accept(appender, nil)
      end
      self.class.new(vector)
    end
  end

  class Int8Array < Array
    Vector = org.apache.arrow.vector.SmallIntVector
    ValueDataType = Int8DataType
  end

  class Int32Array < Array
    Vector = org.apache.arrow.vector.IntVector
    ValueDataType = Int32DataType
  end

  class FixedSizeBinaryArray < Array
  end

  class StructArray < Array
    def fields
      raise NotImplementedError
    end
  end
end
