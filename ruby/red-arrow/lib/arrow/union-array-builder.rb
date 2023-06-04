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
  class UnionArrayBuilder
    def append_values(values, is_valids=nil)
      if is_valids
        is_valids.each_with_index do |is_valid, i|
          if is_valid
            append_value(values[i])
          else
            append_null
          end
        end
      else
        values.each do |value|
          append_value(value)
        end
      end
    end

    alias_method :append_child_raw, :append_child
    def append_child(builder, filed_name=nil)
      @child_infos = nil
      append_child_raw(builder, field_name)
    end

    private
    def child_infos
      @child_infos ||= create_child_infos
    end

    def create_child_infos
      infos = {}
      type = value_data_type
      type.fields.zip(children, type.type_codes).each do |field, child, id|
        infos[field.name] = {
          builder: child,
          id: id,
        }
      end
      infos
    end
  end
end
