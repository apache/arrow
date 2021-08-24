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
  # TODO: Almost codes should be implemented in Apache Arrow C++.
  class TableFormatter
    # @private
    class ColumnFormatter
      attr_reader :column
      attr_reader :head_values
      attr_reader :tail_values
      attr_reader :sample_values
      def initialize(column, head_values, tail_values)
        @column = column
        @head_values = head_values
        @tail_values = tail_values
        @sample_values = head_values + tail_values
        @field_value_widths = {}
      end

      def data_type
        @data_type ||= @column.data_type
      end

      def name
        @name ||= @column.name
      end

      def aligned_name
        @aligned_name ||= format_aligned_name(name, data_type, @sample_values)
      end

      FLOAT_N_DIGITS = 10
      FORMATTED_NULL = "(null)"

      def format_value(value, width=0)
        case value
        when ::Time
          value.iso8601
        when Float
          "%*f" % [[width, FLOAT_N_DIGITS].max, value]
        when Integer
          "%*d" % [width, value]
        when Hash
          formatted_values = data_type.fields.collect do |field|
            field_name = field.name
            field_value_width = compute_field_value_width(field, @sample_values)
            formatted_name = format_value(field_name, 0)
            formatted_value = format_value(value[field_name], field_value_width)
            "#{formatted_name}: #{formatted_value}"
          end
          formatted = "{"
          formatted << formatted_values.join(", ")
          formatted << "}"
          "%-*s" % [width, formatted]
        when nil
          "%*s" % [width, FORMATTED_NULL]
        else
          "%-*s" % [width, value.to_s]
        end
      end

      private
      def compute_field_value_width(field, sample_values)
        unless @field_value_widths.key?(field)
          field_name = field.name
          field_sample_values = sample_values.collect do |v|
            (v || {})[field_name]
          end
          field_aligned_name = format_aligned_name("",
                                                   field.data_type,
                                                   field_sample_values)
          @field_value_widths[field] = field_aligned_name.size
        end
        @field_value_widths[field]
      end

      def format_aligned_name(name, data_type, sample_values)
        case data_type
        when TimestampDataType
          "%*s" % [::Time.now.iso8601.size, name]
        when IntegerDataType
          have_null = false
          have_negative = false
          max_value = nil
          sample_values.each do |value|
            if value.nil?
              have_null = true
            else
              if max_value.nil?
                max_value = value.abs
              else
                max_value = [value.abs, max_value].max
              end
              have_negative = true if value.negative?
            end
          end
          if max_value.nil?
            width = 0
          elsif max_value.zero?
            width = 1
          else
            width = (Math.log10(max_value) + 1).truncate
          end
          width += 1 if have_negative # Need "-"
          width = [width, FORMATTED_NULL.size].max if have_null
          "%*s" % [width, name]
        when FloatDataType, DoubleDataType
          "%*s" % [FLOAT_N_DIGITS, name]
        when StructDataType
          field_widths = data_type.fields.collect do |field|
            field_value_width = compute_field_value_width(field, sample_values)
            field.name.size + ": ".size + field_value_width
          end
          width = "{}".size + field_widths.sum
          if field_widths.size > 0
            width += (", ".size * (field_widths.size - 1))
          end
          "%*s" % [width, name]
        else
          name
        end
      end
    end

    def initialize(table, options={})
      @table = table
      @options = options
    end

    def format
      text = ""
      n_rows = @table.n_rows
      border = @options[:border] || 10

      head_limit = [border, n_rows].min

      tail_start = [border, n_rows - border].max
      tail_limit = n_rows - tail_start

      column_formatters = @table.columns.collect do |column|
        head_values = column.each.take(head_limit)
        if tail_limit > 0
          tail_values = column.reverse_each.take(tail_limit).reverse
        else
          tail_values = []
        end
        ColumnFormatter.new(column, head_values, tail_values)
      end

      format_header(text, column_formatters)
      return text if n_rows.zero?

      n_digits = (Math.log10(n_rows) + 1).truncate
      format_rows(text,
                  column_formatters,
                  column_formatters.collect(&:head_values).transpose,
                  n_digits,
                  0)
      return text if n_rows <= border


      if head_limit != tail_start
        format_ellipsis(text)
      end

      format_rows(text,
                  column_formatters,
                  column_formatters.collect(&:tail_values).transpose,
                  n_digits,
                  tail_start)

      text
    end
  end
end
