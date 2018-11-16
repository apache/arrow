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
  class DataType
    class << self
      def resolve(data_type)
        case data_type
        when DataType
          data_type
        when String, Symbol
          data_type_name = data_type.to_s.capitalize.gsub(/\AUint/, "UInt")
          data_type_class_name = "#{data_type_name}DataType"
          unless Arrow.const_defined?(data_type_class_name)
            raise ArgumentError, "invalid data type: #{data_typeinspect}"
          end
          data_type_class = Arrow.const_get(data_type_class_name)
          data_type_class.new
        else
          raise ArgumentError, "invalid data type: #{data_type.inspect}"
        end
      end
    end
  end
end
