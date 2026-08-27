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
  module ListFieldResolvable
    private
    def resolve_data_type(arg)
      case arg
      when DataType, String, Symbol, ::Array
        DataType.resolve(arg)
      when Hash
        return nil if arg[:name]
        return nil unless arg[:type]
        DataType.resolve(arg)
      else
        nil
      end
    end

    def default_field_name
      "item"
    end

    def resolve_field(arg)
      data_type = resolve_data_type(arg)
      if data_type
        Field.new(default_field_name, data_type)
      elsif arg.is_a?(Hash) and arg.key?(:field)
        description = arg
        description[:field]
      else
        arg
      end
    end
  end
end
