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

require "arrow/struct"

module Arrow
  class StructArray
    def [](i)
      warn("Use #{self.class}\#find_field instead. " +
           "This will returns Arrow::Struct instead of Arrow::Array " +
           "since 0.13.0.")
      get_field(i)
    end

    def get_value(i)
      Struct.new(self, i)
    end

    def find_field(index_or_name)
      case index_or_name
      when String, Symbol
        name = index_or_name
        (@name_to_field ||= build_name_to_field)[name.to_s]
      else
        index = index_or_name
        cached_fields[index]
      end
    end

    private
    def cached_fields
      @fields ||= fields
    end

    def build_name_to_field
      name_to_field = {}
      field_arrays = cached_fields
      value_data_type.fields.each_with_index do |field, i|
        name_to_field[field.name] = field_arrays[i]
      end
      name_to_field
    end
  end
end
