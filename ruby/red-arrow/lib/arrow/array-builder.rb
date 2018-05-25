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

require "date"

module Arrow
  class ArrayBuilder
    class << self
      def build(values)
        if self != ArrayBuilder
          builder = new
          return builder.build(values)
        end

        builder_class = nil
        values.each do |value|
          case value
          when nil
            # Ignore
          when true, false
            return BooleanArray.new(values)
          when String
            return StringArray.new(values)
          when Float
            return DoubleArray.new(values)
          when Integer
            if value < 0
              builder = IntArrayBuilder.new
              return builder.build(values)
            else
              builder_class = UIntArrayBuilder
            end
          when Time
            data_type = TimestampDataType.new(:nano)
            builder = TimestampArrayBuilder.new(data_type)
            return builder.build(values)
          when DateTime
            return Date64Array.new(values)
          when Date
            return Date32Array.new(values)
          else
            return StringArray.new(values)
          end
        end
        if builder_class
          builder_class.new.build(values)
        else
          Arrow::StringArray.new(values)
        end
      end
    end

    def build(values)
      value_convertable = respond_to?(:convert_to_arrow_value, true)
      start_index = 0
      current_index = 0
      status = :value

      values.each do |value|
        if value.nil?
          if status == :value
            if start_index != current_index
              target_values = values[start_index...current_index]
              if value_convertable
                target_values = target_values.collect do |v|
                  convert_to_arrow_value(v)
                end
              end
              append_values(target_values, nil)
              start_index = current_index
            end
            status = :null
          end
        else
          if status == :null
            append_nulls(current_index - start_index)
            start_index = current_index
            status = :value
          end
        end
        current_index += 1
      end
      if start_index != current_index
        if status == :value
          if start_index == 0 and current_index == values.size
            target_values = values
          else
            target_values = values[start_index...current_index]
          end
          if value_convertable
            target_values = target_values.collect do |v|
              convert_to_arrow_value(v)
            end
          end
          append_values(target_values, nil)
        else
          append_nulls(current_index - start_index)
        end
      end

      finish
    end

    def append_nulls(n)
      n.times do
        append_null
      end
    end
  end
end
