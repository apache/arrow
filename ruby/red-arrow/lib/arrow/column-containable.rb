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
  module ColumnContainable
    def columns
      @columns ||= schema.n_fields.times.collect do |i|
        Column.new(self, i)
      end
    end

    def each_column(&block)
      columns.each(&block)
    end

    # @overload [](name)
    #   Find a column that has the given name.
    #
    #   @param name [String, Symbol] The column name to be found.
    #   @return [Column] The found column.
    #
    # @overload [](index)
    #   Find the `index`-th column.
    #
    #   @param index [Integer] The index to be found.
    #   @return [Column] The found column.
    def find_column(name_or_index)
      case name_or_index
      when String, Symbol
        name = name_or_index.to_s
        index = schema.get_field_index(name)
        return nil if index == -1
        Column.new(self, index)
      when Integer
        index = name_or_index
        index += n_columns if index < 0
        return nil if index < 0 or index >= n_columns
        Column.new(self, index)
      else
        message = "column name or index must be String, Symbol or Integer: "
        message << name_or_index.inspect
        raise ArgumentError, message
      end
    end

    # Selects columns that are selected by `selectors` and/or `block`
    # and creates a new container only with the selected columns.
    #
    # @param selectors [Array<String, Symbol, Integer, Range>]
    #   If a selector is `String`, `Symbol` or `Integer`, the selector
    #   selects a column by {#find_column}.
    #
    #   If a selector is `Range`, the selector selects columns by `::Array#[]`.
    # @yield [column] Gives a column to the block to select columns.
    #   This uses `::Array#select`.
    # @yieldparam column [Column] A target column.
    # @yieldreturn [Boolean] Whether the given column is selected or not.
    # @return [self.class] The newly created container that only has selected
    #   columns.
    def select_columns(*selectors, &block)
      if selectors.empty?
        return to_enum(__method__) unless block_given?
        selected_columns = columns.select(&block)
      else
        selected_columns = []
        selectors.each do |selector|
          case selector
          when Range
            selected_columns.concat(columns[selector])
          else
            column = find_column(selector)
            if column.nil?
              case selector
              when String, Symbol
                message = "unknown column: #{selector.inspect}: #{inspect}"
                raise KeyError.new(message)
              else
                message = "out of index (0..#{n_columns - 1}): "
                message << "#{selector.inspect}: #{inspect}"
                raise IndexError.new(message)
              end
            end
            selected_columns << column
          end
        end
        selected_columns = selected_columns.select(&block) if block_given?
      end
      self.class.new(selected_columns)
    end

    # @overload [](name)
    #   Find a column that has the given name.
    #
    #   @param name [String, Symbol] The column name to be found.
    #   @return [Column] The found column.
    #   @see #find_column
    #
    # @overload [](index)
    #   Find the `index`-th column.
    #
    #   @param index [Integer] The index to be found.
    #   @return [Column] The found column.
    #   @see #find_column
    #
    # @overload [](range)
    #   Selects columns that are in `range` and creates a new container
    #   only with the selected columns.
    #
    #   @param range [Range] The range to be selected.
    #   @return [self.class] The newly created container that only has selected
    #     columns.
    #   @see #select_columns
    #
    # @overload [](selectors)
    #   Selects columns that are selected by `selectors` and creates a
    #   new container only with the selected columns.
    #
    #   @param selectors [Array] The selectors that are used to select columns.
    #   @return [self.class] The newly created container that only has selected
    #     columns.
    #   @see #select_columns
    def [](selector)
      case selector
      when ::Array
        select_columns(*selector)
      when Range
        select_columns(selector)
      else
        find_column(selector)
      end
    end

    # Return column names in this object.
    #
    # @return [::Array<String>] column names.
    #
    # @since 11.0.0
    def column_names
      @column_names ||= columns.collect(&:name)
    end
  end
end
