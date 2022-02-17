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
    def format_header(text, column_formatters)
      column_formatters.each do |column_formatter|
        text << "\t"
        text << column_formatter.aligned_name
      end
      text << "\n"
    end

    def format_rows(text, column_formatters, rows, n_digits, start_offset)
      rows.each_with_index do |row, nth_row|
        text << ("%*d" % [n_digits, start_offset + nth_row])
        row.each_with_index do |column_value, nth_column|
          text << "\t"
          column_formatter = column_formatters[nth_column]
          aligned_name = column_formatter.aligned_name
          text << column_formatter.format_value(column_value, aligned_name.size)
        end
        text << "\n"
      end
    end

    def format_ellipsis(text)
      text << "...\n"
    end
  end
end
