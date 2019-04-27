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

require "arrow/group"
require "arrow/record-containable"

module Arrow
  class Table
    include RecordContainable

    class << self
      def load(path, options={})
        TableLoader.load(path, options)
      end
    end

    alias_method :initialize_raw, :initialize
    private :initialize_raw

    # Creates a new {Arrow::Table}.
    #
    # @overload initialize(columns)
    #
    #   @param columns [::Array<Arrow::Column>] The columns of the table.
    #
    #   @example Create a table from columns
    #     count_field = Arrow::Field.new("count", :uint32)
    #     count_array = Arrow::UInt32Array.new([0, 2, nil, 4])
    #     count_column = Arrow::Column.new(count_field, count_array)
    #     visible_field = Arrow::Field.new("visible", :boolean)
    #     visible_array = Arrow::BooleanArray.new([true, nil, nil, false])
    #     visible_column = Arrow::Column.new(visible_field, visible_array)
    #     Arrow::Table.new([count_column, visible_column])
    #
    # @overload initialize(raw_table)
    #
    #   @param raw_table [Hash<String, Arrow::Array>]
    #     The pairs of column name and values of the table. Column values is
    #     `Arrow::Array`.
    #
    #   @example Create a table from column name and values
    #     Arrow::Table.new("count" => Arrow::UInt32Array.new([0, 2, nil, 4]),
    #                      "visible" => Arrow::BooleanArray.new([true, nil, nil, false]))
    #
    # @overload initialize(raw_table)
    #
    #   @param raw_table [Hash<String, Arrow::ChunkedArray>]
    #     The pairs of column name and values of the table. Column values is
    #     `Arrow::ChunkedArray`.
    #
    #   @example Create a table from column name and values
    #     count_chunks = [
    #       Arrow::UInt32Array.new([0, 2]),
    #       Arrow::UInt32Array.new([nil, 4]),
    #     ]
    #     visible_chunks = [
    #       Arrow::BooleanArray.new([true]),
    #       Arrow::BooleanArray.new([nil, nil, false]),
    #     ]
    #     Arrow::Table.new("count" => Arrow::ChunkedArray.new(count_chunks),
    #                      "visible" => Arrow::ChunkedArray.new(visible_chunks))
    #
    # @overload initialize(schema, columns)
    #
    #   @param schema [Arrow::Schema] The schema of the table.
    #     You can also specify schema as primitive Ruby objects.
    #     See {Arrow::Schema#initialize} for details.
    #
    #   @param columns [::Array<Arrow::Column>] The data of the table.
    #
    #   @example Create a table from schema and columns
    #     count_field = Arrow::Field.new("count", :uint32)
    #     count_array = Arrow::UInt32Array.new([0, 2, nil, 4])
    #     count_column = Arrow::Column.new(count_field, count_array)
    #     visible_field = Arrow::Field.new("visible", :boolean)
    #     visible_array = Arrow::BooleanArray.new([true, nil, nil, false])
    #     visible_column = Arrow::Column.new(visible_field, visible_array)
    #     Arrow::Table.new(Arrow::Schema.new([count_field, visible_field]),
    #                      [count_column, visible_column])
    #
    # @overload initialize(schema, arrays)
    #
    #   @param schema [Arrow::Schema] The schema of the table.
    #     You can also specify schema as primitive Ruby objects.
    #     See {Arrow::Schema#initialize} for details.
    #
    #   @param arrays [::Array<Arrow::Array>] The data of the table.
    #
    #   @example Create a table from schema and arrays
    #     count_field = Arrow::Field.new("count", :uint32)
    #     count_array = Arrow::UInt32Array.new([0, 2, nil, 4])
    #     visible_field = Arrow::Field.new("visible", :boolean)
    #     visible_array = Arrow::BooleanArray.new([true, nil, nil, false])
    #     Arrow::Table.new(Arrow::Schema.new([count_field, visible_field]),
    #                      [count_array, visible_array])
    #
    # @overload initialize(schema, record_batches)
    #
    #   @param schema [Arrow::Schema] The schema of the table.
    #     You can also specify schema as primitive Ruby objects.
    #     See {Arrow::Schema#initialize} for details.
    #
    #   @param arrays [::Array<Arrow::RecordBatch>] The data of the table.
    #
    #   @example Create a table from schema and record batches
    #     count_field = Arrow::Field.new("count", :uint32)
    #     visible_field = Arrow::Field.new("visible", :boolean)
    #     schema = Arrow::Schema.new([count_field, visible_field])
    #     record_batches = [
    #       Arrow::RecordBatch.new(schema, [[0, true], [2, nil], [nil, nil]]),
    #       Arrow::RecordBatch.new(schema, [[4, false]]),
    #     ]
    #     Arrow::Table.new(schema, record_batches)
    #
    # @overload initialize(schema, raw_records)
    #
    #   @param schema [Arrow::Schema] The schema of the table.
    #     You can also specify schema as primitive Ruby objects.
    #     See {Arrow::Schema#initialize} for details.
    #
    #   @param arrays [::Array<::Array>] The data of the table as primitive
    #     Ruby objects.
    #
    #   @example Create a table from schema and raw records
    #     schema = {
    #       count: :uint32,
    #       visible: :boolean,
    #     }
    #     raw_records = [
    #       [0, true],
    #       [2, nil],
    #       [nil, nil],
    #       [4, false],
    #     ]
    #     Arrow::Table.new(schema, raw_records)
    def initialize(*args)
      n_args = args.size
      case n_args
      when 1
        if args[0][0].is_a?(Column)
          values = args[0]
          fields = values.collect(&:field)
          schema = Schema.new(fields)
        else
          raw_table = args[0]
          fields = []
          values = []
          raw_table.each do |name, array|
            field = Field.new(name.to_s, array.value_data_type)
            fields << field
            values << Column.new(field, array)
          end
          schema = Schema.new(fields)
        end
      when 2
        schema = args[0]
        schema = Schema.new(schema) unless schema.is_a?(Schema)
        values = args[1]
        if values[0].is_a?(::Array)
          values = [RecordBatch.new(schema, values)]
        end
      else
        message = "wrong number of arguments (given, #{n_args}, expected 1..2)"
        raise ArgumentError, message
      end
      initialize_raw(schema, values)
    end

    def columns
      @columns ||= n_columns.times.collect {|i| get_column(i)}
    end

    def each_record_batch
      return to_enum(__method__) unless block_given?

      reader = TableBatchReader.new(self)
      while record_batch = reader.read_next
        yield(record_batch)
      end
    end

    alias_method :size, :n_rows
    alias_method :length, :n_rows

    alias_method :[], :find_column

    # TODO
    #
    # @return [Arrow::Table]
    def slice(*args)
      slicers = []
      expected_n_args = nil
      case args.size
      when 0
        expected_n_args = "1..2" unless block_given?
      when 1
        slicers << args[0]
      when 2
        from, to = args
        slicers << (from...(from + to))
      else
        if block_given?
          expected_n_args = "0..2"
        else
          expected_n_args = "1..2"
        end
      end
      if expected_n_args
        message = "wrong number of arguments " +
          "(given #{args.size}, expected #{expected_n_args})"
        raise ArgumentError, message
      end

      if block_given?
        block_slicer = yield(Slicer.new(self))
        case block_slicer
        when nil
          # Ignore
        when ::Array
          slicers.concat(block_slicer)
        else
          slicers << block_slicer
        end
      end

      ranges = []
      slicers.each do |slicer|
        slicer = slicer.evaluate if slicer.respond_to?(:evaluate)
        case slicer
        when Integer
          slicer += n_rows if slicer < 0
          ranges << [slicer, slicer]
        when Range
          from = slicer.first
          to = slicer.last
          to -= 1 if slicer.exclude_end?
          from += n_rows if from < 0
          to += n_rows if to < 0
          ranges << [from, to]
        when ::Array
          boolean_array_to_slice_ranges(slicer, 0, ranges)
        when ChunkedArray
          offset = 0
          slicer.each_chunk do |array|
            boolean_array_to_slice_ranges(array, offset, ranges)
            offset += array.length
          end
        when BooleanArray
          boolean_array_to_slice_ranges(slicer, 0, ranges)
        else
          message = "slicer must be Integer, Range, (from, to), " +
            "Arrow::ChunkedArray of Arrow::BooleanArray, " +
            "Arrow::BooleanArray or Arrow::Slicer::Condition: #{slicer.inspect}"
          raise ArgumentError, message
        end
      end
      slice_by_ranges(ranges)
    end

    # TODO
    #
    # @return [Arrow::Table]
    def merge(other)
      added_columns = {}
      removed_columns = {}

      case other
      when Hash
        other.each do |name, value|
          name = name.to_s
          if value
            added_columns[name] = ensure_column(name, value)
          else
            removed_columns[name] = true
          end
        end
      when Table
        added_columns = {}
        other.columns.each do |column|
          added_columns[column.name] = column
        end
      else
        message = "merge target must be Hash or Arrow::Table: " +
          "<#{other.inspect}>: #{inspect}"
        raise ArgumentError, message
      end

      new_columns = []
      columns.each do |column|
        column_name = column.name
        new_column = added_columns.delete(column_name)
        if new_column
          new_columns << new_column
          next
        end
        next if removed_columns.key?(column_name)
        new_columns << column
      end
      added_columns.each do |name, new_column|
        new_columns << new_column
      end
      new_fields = new_columns.collect do |new_column|
        new_column.field
      end
      self.class.new(Schema.new(new_fields), new_columns)
    end

    alias_method :remove_column_raw, :remove_column
    def remove_column(name_or_index)
      case name_or_index
      when String, Symbol
        name = name_or_index.to_s
        index = columns.index {|column| column.name == name}
        if index.nil?
          message = "unknown column: #{name_or_index.inspect}: #{inspect}"
          raise KeyError.new(message)
        end
      else
        index = name_or_index
        index += n_columns if index < 0
        if index < 0 or index >= n_columns
          message = "out of index (0..#{n_columns - 1}): " +
            "#{name_or_index.inspect}: #{inspect}"
          raise IndexError.new(message)
        end
      end
      remove_column_raw(index)
    end

    # TODO
    #
    # @return [Arrow::Table]
    def select_columns(*selectors, &block)
      if selectors.empty?
        return to_enum(__method__) unless block_given?
        selected_columns = columns.select(&block)
      else
        selected_columns = []
        selectors.each do |selector|
          case selector
          when String, Symbol
            column = find_column(selector)
            if column.nil?
              message = "unknown column: #{selector.inspect}: #{inspect}"
              raise KeyError.new(message)
            end
            selected_columns << column
          when Range
            selected_columns.concat(columns[selector])
          else
            column = columns[selector]
            if column.nil?
              message = "out of index (0..#{n_columns - 1}): " +
              "#{selector.inspect}: #{inspect}"
              raise IndexError.new(message)
            end
            selected_columns << column
          end
        end
        selected_columns = selected_columns.select(&block) if block_given?
      end
      self.class.new(selected_columns)
    end

    # Experimental
    def group(*keys)
      Group.new(self, keys)
    end

    # Experimental
    def window(size: nil)
      RollingWindow.new(self, size)
    end

    def save(path, options={})
      saver = TableSaver.new(self, path, options)
      saver.save
    end

    def pack
      packed_columns = columns.collect do |column|
        column.pack
      end
      self.class.new(schema, packed_columns)
    end

    alias_method :to_s_raw, :to_s
    def to_s(options={})
      format = options[:format]
      case format
      when :column
        return to_s_raw
      when :list
        formatter_class = TableListFormatter
      when :table, nil
        formatter_class = TableTableFormatter
      else
        message = ":format must be :column, :list, :table or nil"
        raise ArgumentError, "#{message}: <#{format.inspect}>"
      end
      formatter = formatter_class.new(self, options)
      formatter.format
    end

    alias_method :inspect_raw, :inspect
    def inspect
      "#{super}\n#{to_s}"
    end

    def respond_to_missing?(name, include_private)
      return true if find_column(name)
      super
    end

    def method_missing(name, *args, &block)
      if args.empty?
        column = find_column(name)
        return column if column
      end
      super
    end

    private
    def boolean_array_to_slice_ranges(array, offset, ranges)
      in_target = false
      target_start = nil
      array.each_with_index do |is_target, i|
        if is_target
          unless in_target
            target_start = offset + i
            in_target = true
          end
        else
          if in_target
            ranges << [target_start, offset + i - 1]
            target_start = nil
            in_target = false
          end
        end
      end
      if in_target
        ranges << [target_start, offset + array.length - 1]
      end
    end

    # TODO: Almost codes should be implemented in Apache Arrow C++.
    def slice_by_ranges(ranges)
      sliced_columns = columns.collect do |column|
        chunks = []
        arrays = column.data.each_chunk.to_a
        offset = 0
        offset_in_array = 0
        ranges.each do |from, to|
          range_size = to - from + 1
          while range_size > 0
            while offset + arrays.first.length - offset_in_array < from
              offset += arrays.first.length - offset_in_array
              arrays.shift
              offset_in_array = 0
            end
            if offset < from
              skipped_size = from - offset
              offset += skipped_size
              offset_in_array += skipped_size
            end
            array = arrays.first
            array_length = array.length
            rest_length = array_length - offset_in_array
            if rest_length <= range_size
              chunks << array.slice(offset_in_array, array_length)
              offset += rest_length
              range_size -= rest_length
              offset_in_array = 0
              arrays.shift
            else
              chunks << array.slice(offset_in_array, range_size)
              offset += range_size
              offset_in_array += range_size
              range_size = 0
            end
          end
        end
        Column.new(column.field, ChunkedArray.new(chunks))
      end

      self.class.new(schema, sliced_columns)
    end

    def ensure_column(name, data)
      case data
      when Array
        field = Field.new(name, data.value_data_type)
        Column.new(field, data)
      when Column
        data
      else
        message = "column must be Arrow::Array or Arrow::Column: " +
          "<#{name}>: <#{data.inspect}>: #{inspect}"
        raise ArgumentError, message
      end
    end
  end
end
