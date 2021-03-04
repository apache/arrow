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
  class Time32ArrayBuilder
    class << self
      def build(unit_or_data_type, values)
        builder = new(unit_or_data_type)
        builder.build(values)
      end
    end

    alias_method :initialize_raw, :initialize
    def initialize(unit_or_data_type)
      case unit_or_data_type
      when DataType
        data_type = unit_or_data_type
      else
        unit = unit_or_data_type
        data_type = Time32DataType.new(unit)
      end
      initialize_raw(data_type)
    end

    def unit
      @unit ||= value_data_type.unit
    end

    private
    def convert_to_arrow_value(value)
      return value unless value.is_a?(Time)
      value.cast(unit).value
    end
  end
end
