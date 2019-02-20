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

require "arrow/bigdecimal-extension"

module Arrow
  class Decimal128ArrayBuilder
    class << self
      def build(data_type, values)
        builder = new(data_type)
        builder.build(values)
      end
    end

    alias_method :append_value_raw, :append_value
    def append_value(value)
      case value
      when nil
        return append_null
      when String
        value = Decimal128.new(value)
      when Float
        value = Decimal128.new(value.to_s)
      when BigDecimal
        value = value.to_arrow
      end
      append_value_raw(value)
    end

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
          if value.nil?
            append_null
          else
            append_value(value)
          end
        end
      end
    end
  end
end
