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
  class Field
    alias_method :initialize_raw, :initialize
    def initialize(name, data_type)
      case data_type
      when String, Symbol
        data_type_name = data_type.to_s.capitalize.gsub(/\AUint/, "UInt")
        data_type_class_name = "#{data_type_name}DataType"
        if Arrow.const_defined?(data_type_class_name)
          data_type_class = Arrow.const_get(data_type_class_name)
          data_type = data_type_class.new
        end
      end
      initialize_raw(name, data_type)
    end
  end
end
