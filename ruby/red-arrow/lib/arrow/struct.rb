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
  class Struct
    attr_accessor :index
    def initialize(array, index)
      @array = array
      @index = index
    end

    def [](field_name_or_field_index)
      field = @array.find_field(field_name_or_field_index)
      return nil if field.nil?
      field[@index]
    end

    def fields
      @array.value_data_type.fields
    end

    def values
      @array.fields.collect do |field|
        field[@index]
      end
    end

    def to_a
      values
    end

    def to_h
      attributes = {}
      field_arrays = @array.fields
      fields.each_with_index do |field, i|
        attributes[field.name] = field_arrays[i][@index]
      end
      attributes
    end

    def respond_to_missing?(name, include_private)
      return true if @array.find_field(name)
      super
    end

    def method_missing(name, *args, &block)
      if args.empty?
        field = @array.find_field(name)
        return field[@index] if field
      end
      super
    end

    def ==(other)
      other.is_a?(self.class) and
        @array == other.array and
        @index == other.index
    end

    protected
    def array
      @array
    end
  end
end
