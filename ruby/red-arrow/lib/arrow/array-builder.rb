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

        builder_info = nil
        values.each do |value|
          builder_info = detect_builder_info(value, builder_info)
          break if builder_info and builder_info[:detected]
        end
        if builder_info
          builder = builder_info[:builder]
          if builder.nil? and builder_info[:builder_type]
            builder = create_builder(builder_info)
          end
        end
        if builder
          builder.build(values)
        else
          Arrow::StringArray.new(values)
        end
      end

      def buildable?(args)
        args.size == method(:build).arity
      end

      private
      def detect_builder_info(value, builder_info)
        case value
        when nil
          builder_info
        when true, false
          {
            builder: BooleanArrayBuilder.new,
            detected: true,
          }
        when String
          {
            builder: StringArrayBuilder.new,
            detected: true,
          }
        when Symbol
          {
            builder: StringDictionaryArrayBuilder.new,
            detected: true,
          }
        when Float
          {
            builder: DoubleArrayBuilder.new,
            detected: true,
          }
        when Integer
          if value < 0
            {
              builder: IntArrayBuilder.new,
              detected: true,
            }
          else
            {
              builder: UIntArrayBuilder.new,
            }
          end
        when Time
          data_type = value.data_type
          case data_type.unit
          when TimeUnit::SECOND
            builder_info || {
              builder: Time32ArrayBuilder.new(data_type)
            }
          when TimeUnit::MILLI
            if builder_info and builder_info[:builder].is_a?(Time64ArrayBuilder)
              builder_info
            else
              {
                builder: Time32ArrayBuilder.new(data_type),
              }
            end
          when TimeUnit::MICRO
            {
              builder: Time64ArrayBuilder.new(data_type),
            }
          when TimeUnit::NANO
            {
              builder: Time64ArrayBuilder.new(data_type),
              detected: true
            }
          end
        when ::Time
          data_type = TimestampDataType.new(:nano)
          {
            builder: TimestampArrayBuilder.new(data_type),
            detected: true,
          }
        when DateTime
          {
            builder: Date64ArrayBuilder.new,
            detected: true,
          }
        when Date
          {
            builder: Date32ArrayBuilder.new,
            detected: true,
          }
        when BigDecimal
          builder_info ||= {}
          if builder_info[:builder] or value.nan? or value.infinite?
            {
              builder: StringArrayBuilder.new,
              detected: true,
            }
          else
            precision = [builder_info[:precision] || 0, value.precision].max
            scale = [builder_info[:scale] || 0, value.scale].max
            if precision <= Decimal128DataType::MAX_PRECISION
              {
                builder_type: :decimal128,
                precision: precision,
                scale: scale,
              }
            else
              {
                builder_type: :decimal256,
                precision: precision,
                scale: scale,
              }
            end
          end
        when ::Array
          sub_builder_info = nil
          value.each do |sub_value|
            sub_builder_info = detect_builder_info(sub_value, sub_builder_info)
            break if sub_builder_info and sub_builder_info[:detected]
          end
          if sub_builder_info and sub_builder_info[:detected]
            sub_value_data_type = sub_builder_info[:builder].value_data_type
            field = Field.new("item", sub_value_data_type)
            {
              builder: ListArrayBuilder.new(ListDataType.new(field)),
              detected: true,
            }
          else
            builder_info
          end
        else
          {
            builder: StringArrayBuilder.new,
            detected: true,
          }
        end
      end

      def create_builder(builder_info)
        builder_type = builder_info[:builder_type]
        case builder_type
        when :decimal128
          data_type = Decimal128DataType.new(builder_info[:precision],
                                             builder_info[:scale])
          Decimal128ArrayBuilder.new(data_type)
        when :decimal256
          data_type = Decimal256DataType.new(builder_info[:precision],
                                             builder_info[:scale])
          Decimal256ArrayBuilder.new(data_type)
        else
          nil
        end
      end
    end

    def build(values)
      append(*values)
      finish
    end

    # @since 0.12.0
    def append(*values)
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
    end
  end
end
