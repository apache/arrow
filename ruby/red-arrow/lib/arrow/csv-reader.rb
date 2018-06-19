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

require "csv"

module Arrow
  class CSVReader
    def initialize(csv)
      @csv = csv
    end

    def read
      values_set = []
      @csv.each do |row|
        if row.is_a?(CSV::Row)
          row = row.collect(&:last)
        end
        row.each_with_index do |value, i|
          values = (values_set[i] ||= [])
          values << value
        end
      end
      return nil if values_set.empty?

      arrays = values_set.collect.with_index do |values, i|
        ArrayBuilder.build(values)
      end
      if @csv.headers
        names = @csv.headers
      else
        names = arrays.size.times.collect(&:to_s)
      end
      raw_table = {}
      names.each_with_index do |name, i|
        raw_table[name] = arrays[i]
      end
      Table.new(raw_table)
    end
  end
end
