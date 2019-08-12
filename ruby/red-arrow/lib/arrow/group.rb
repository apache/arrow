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
  # Experimental
  #
  # TODO: Almost codes should be implemented in Apache Arrow C++.
  class Group
    def initialize(table, keys)
      @table = table
      @keys = keys
    end

    def count
      key_names = @keys.collect(&:to_s)
      target_columns = @table.columns.reject do |column|
        key_names.include?(column.name)
      end
      aggregate(target_columns) do |column, indexes|
        n = 0
        indexes.each do |index|
          n += 1 unless column.null?(index)
        end
        n
      end
    end

    def sum
      key_names = @keys.collect(&:to_s)
      target_columns = @table.columns.reject do |column|
        key_names.include?(column.name) or
          not column.data_type.is_a?(NumericDataType)
      end
      aggregate(target_columns) do |column, indexes|
        n = 0
        indexes.each do |index|
          value = column[index]
          n += value unless value.nil?
        end
        n
      end
    end

    def average
      key_names = @keys.collect(&:to_s)
      target_columns = @table.columns.reject do |column|
        key_names.include?(column.name) or
          not column.data_type.is_a?(NumericDataType)
      end
      aggregate(target_columns) do |column, indexes|
        average = 0.0
        n = 0
        indexes.each do |index|
          value = column[index]
          unless value.nil?
            n += 1
            average += (value - average) / n
          end
        end
        average
      end
    end

    def min
      key_names = @keys.collect(&:to_s)
      target_columns = @table.columns.reject do |column|
        key_names.include?(column.name) or
          not column.data_type.is_a?(NumericDataType)
      end
      aggregate(target_columns) do |column, indexes|
        n = nil
        indexes.each do |index|
          value = column[index]
          next if value.nil?
          n ||= value
          n = value if value < n
        end
        n
      end
    end

    def max
      key_names = @keys.collect(&:to_s)
      target_columns = @table.columns.reject do |column|
        key_names.include?(column.name) or
          not column.data_type.is_a?(NumericDataType)
      end
      aggregate(target_columns) do |column, indexes|
        n = nil
        indexes.each do |index|
          value = column[index]
          next if value.nil?
          n ||= value
          n = value if value > n
        end
        n
      end
    end

    private
    def aggregate(target_columns)
      sort_values = @table.n_rows.times.collect do |i|
        key_values = @keys.collect do |key|
          @table[key][i]
        end
        [key_values, i]
      end
      sorted = sort_values.sort_by do |key_values, i|
        key_values
      end

      grouped_keys = []
      aggregated_arrays_raw = []
      target_columns.size.times do
        aggregated_arrays_raw << []
      end
      indexes = []
      sorted.each do |key_values, i|
        if grouped_keys.empty?
          grouped_keys << key_values
          indexes.clear
          indexes << i
        else
          if key_values == grouped_keys.last
            indexes << i
          else
            grouped_keys << key_values
            target_columns.each_with_index do |column, j|
              aggregated_arrays_raw[j] << yield(column, indexes)
            end
            indexes.clear
            indexes << i
          end
        end
      end
      target_columns.each_with_index do |column, j|
        aggregated_arrays_raw[j] << yield(column, indexes)
      end

      grouped_key_arrays_raw = grouped_keys.transpose
      fields = []
      arrays = []
      @keys.each_with_index do |key, i|
        key_column = @table[key]
        key_column_array_raw = grouped_key_arrays_raw[i]
        key_column_array = key_column.data_type.build_array(key_column_array_raw)
        fields << key_column.field
        arrays << key_column_array
      end
      target_columns.each_with_index do |column, i|
        array = ArrayBuilder.build(aggregated_arrays_raw[i])
        arrays << array
        fields << Field.new(column.field.name, array.value_data_type)
      end
      Table.new(fields, arrays)
    end
  end
end
