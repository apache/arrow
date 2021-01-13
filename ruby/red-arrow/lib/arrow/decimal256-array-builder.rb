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
  class Decimal256ArrayBuilder
    class << self
      # @since 3.0.0
      def build(data_type, values)
        builder = new(data_type)
        builder.build(values)
      end
    end

    alias_method :append_value_raw, :append_value
    # @since 3.0.0
    def append_value(value)
      append_value_raw(normalize_value(value))
    end

    alias_method :append_values_raw, :append_values
    # @since 3.0.0
    def append_values(values, is_valids=nil)
      if values.is_a?(::Array)
        values = values.collect do |value|
          normalize_value(value)
        end
        append_values_raw(values, is_valids)
      else
        append_values_packed(values, is_valids)
      end
    end

    private
    def normalize_value(value)
      case value
      when String
        Decimal256.new(value)
      when Float
        Decimal256.new(value.to_s)
      when BigDecimal
        Decimal256.new(value.to_s)
      else
        value
      end
    end
  end
end
