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

require "time"

module Arrow
  # TODO: Almost codes should be implemented in Apache Arrow C++.
  class TableTableFormatter < TableFormatter
    private
    def format_header(text, columns)
      columns.each do |column|
        text << "\t"
        text << format_column_name(column)
      end
      text << "\n"
    end

    FLOAT_N_DIGITS = 10
    def format_column_name(column)
      case column.data_type
      when TimestampDataType
        "%*s" % [Time.now.iso8601.size, column.name]
      when FloatDataType, DoubleDataType
        "%*s" % [FLOAT_N_DIGITS, column.name]
      else
        column.name
      end
    end

    def format_rows(text, columns, rows, n_digits, start_offset)
      rows.each_with_index do |row, nth_row|
        text << ("%*d" % [n_digits, start_offset + nth_row])
        row.each_with_index do |column_value, nth_column|
          text << "\t"
          column = columns[nth_column]
          text << format_column_value(column, column_value)
        end
        text << "\n"
      end
    end

    def format_column_value(column, value)
      case value
      when Time
        value.iso8601
      when Float
        "%*f" % [[column.name.size, FLOAT_N_DIGITS].max, value]
      when Integer
        "%*d" % [column.name.size, value]
      else
        "%-*s" % [column.name.size, value.to_s]
      end
    end

    def format_ellipsis(text)
      text << "...\n"
    end
  end
end
