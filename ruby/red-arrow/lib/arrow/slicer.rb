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
  class Slicer
    def initialize(table)
      @table = table
    end

    def [](column_name)
      column = @table[column_name]
      return nil if column.nil?
      ColumnCondition.new(column)
    end

    def respond_to_missing?(name, include_private)
      return true if self[name]
      super
    end

    def method_missing(name, *args, &block)
      if args.empty?
        column_condition = self[name]
        return column_condition if column_condition
      end
      super
    end

    class Condition
      def evaluate
        message = "Slicer::Condition must define \#evaluate: #{inspect}"
        raise NotImplementedError.new(message)
      end

      def &(condition)
        AndCondition.new(self, condition)
      end

      def |(condition)
        OrCondition.new(self, condition)
      end

      def ^(condition)
        XorCondition.new(self, condition)
      end
    end

    class LogicalCondition < Condition
      def initialize(condition1, condition2)
        @condition1 = condition1
        @condition2 = condition2
      end

      def evaluate
        values1 = @condition1.evaluate.each
        values2 = @condition2.evaluate.each
        raw_array = []
        begin
          loop do
            value1 = values1.next
            value2 = values2.next
            if value1.nil? or value2.nil?
              raw_array << nil
            else
              raw_array << evaluate_value(value1, value2)
            end
          end
        rescue StopIteration
        end
        BooleanArray.new(raw_array)
      end
    end

    class AndCondition < LogicalCondition
      private
      def evaluate_value(value1, value2)
        value1 and value2
      end
    end

    class OrCondition < LogicalCondition
      private
      def evaluate_value(value1, value2)
        value1 or value2
      end
    end

    class XorCondition < LogicalCondition
      private
      def evaluate_value(value1, value2)
        value1 ^ value2
      end
    end

    class ColumnCondition < Condition
      def initialize(column)
        @column = column
      end

      def evaluate
        data = @column.data

        case @column.data_type
        when BooleanDataType
          data
        else
          if data.n_chunks == 1
            data.get_chunk(0).cast(BooleanDataType.new, nil)
          else
            arrays = data.each_chunk.collect do |chunk|
              chunk.cast(BooleanDataType.new, nil)
            end
            ChunkedArray.new(arrays)
          end
        end
      end

      def !@
        NotColumnCondition.new(@column)
      end

      def null?
        self == nil
      end

      def valid?
        self != nil
      end

      def ==(value)
        EqualCondition.new(@column, value)
      end

      def !=(value)
        NotEqualCondition.new(@column, value)
      end

      def <(value)
        LessCondition.new(@column, value)
      end

      def <=(value)
        LessEqualCondition.new(@column, value)
      end

      def >(value)
        GreaterCondition.new(@column, value)
      end

      def >=(value)
        GreaterEqualCondition.new(@column, value)
      end

      def in?(values)
        InCondition.new(@column, values)
      end

      def select(&block)
        SelectCondition.new(@column, block)
      end

      def reject(&block)
        RejectCondition.new(@column, block)
      end
    end

    class NotColumnCondition < Condition
      def initialize(column)
        @column = column
      end

      def evaluate
        data = @column.data
        raw_array = []
        data.each_chunk do |chunk|
          if chunk.is_a?(BooleanArray)
            boolean_array = chunk
          else
            boolean_array = chunk.cast(BooleanDataType.new, nil)
          end
          boolean_array.each do |value|
            if value.nil?
              raw_array << value
            else
              raw_array << !value
            end
          end
        end
        BooleanArray.new(raw_array)
      end

      def !@
        ColumnCondition.new(@column)
      end
    end

    class EqualCondition < Condition
      def initialize(column, value)
        @column = column
        @value = value
      end

      def !@
        NotEqualCondition.new(@column, @value)
      end

      def evaluate
        case @value
        when nil
          raw_array = @column.collect(&:nil?)
          BooleanArray.new(raw_array)
        else
          raw_array = @column.collect do |value|
            if value.nil?
              nil
            else
              @value == value
            end
          end
          BooleanArray.new(raw_array)
        end
      end
    end

    class NotEqualCondition < Condition
      def initialize(column, value)
        @column = column
        @value = value
      end

      def !@
        EqualCondition.new(@column, @value)
      end

      def evaluate
        case @value
        when nil
          if @column.n_nulls.zero?
            raw_array = [true] * @column.length
          else
            raw_array = @column.length.times.collect do |i|
              @column.valid?(i)
            end
          end
          BooleanArray.new(raw_array)
        else
          raw_array = @column.collect do |value|
            if value.nil?
              nil
            else
              @value != value
            end
          end
          BooleanArray.new(raw_array)
        end
      end
    end

    class LessCondition < Condition
      def initialize(column, value)
        @column = column
        @value = value
      end

      def !@
        GreaterEqualCondition.new(@column, @value)
      end

      def evaluate
        raw_array = @column.collect do |value|
          if value.nil?
            nil
          else
            @value > value
          end
        end
        BooleanArray.new(raw_array)
      end
    end

    class LessEqualCondition < Condition
      def initialize(column, value)
        @column = column
        @value = value
      end

      def !@
        GreaterCondition.new(@column, @value)
      end

      def evaluate
        raw_array = @column.collect do |value|
          if value.nil?
            nil
          else
            @value >= value
          end
        end
        BooleanArray.new(raw_array)
      end
    end

    class GreaterCondition < Condition
      def initialize(column, value)
        @column = column
        @value = value
      end

      def !@
        LessEqualCondition.new(@column, @value)
      end

      def evaluate
        raw_array = @column.collect do |value|
          if value.nil?
            nil
          else
            @value < value
          end
        end
        BooleanArray.new(raw_array)
      end
    end

    class GreaterEqualCondition < Condition
      def initialize(column, value)
        @column = column
        @value = value
      end

      def !@
        LessCondition.new(@column, @value)
      end

      def evaluate
        raw_array = @column.collect do |value|
          if value.nil?
            nil
          else
            @value <= value
          end
        end
        BooleanArray.new(raw_array)
      end
    end

    class InCondition < Condition
      def initialize(column, values)
        @column = column
        @values = values
      end

      def !@
        NotInCondition.new(@column, @values)
      end

      def evaluate
        values_index = {}
        @values.each do |value|
          values_index[value] = true
        end
        raw_array = @column.collect do |value|
          if value.nil?
            nil
          else
            values_index.key?(value)
          end
        end
        BooleanArray.new(raw_array)
      end
    end

    class NotInCondition < Condition
      def initialize(column, values)
        @column = column
        @values = values
      end

      def !@
        InCondition.new(@column, @values)
      end

      def evaluate
        values_index = {}
        @values.each do |value|
          values_index[value] = true
        end
        raw_array = @column.collect do |value|
          if value.nil?
            nil
          else
            not values_index.key?(value)
          end
        end
        BooleanArray.new(raw_array)
      end
    end

    class SelectCondition < Condition
      def initialize(column, block)
        @column = column
        @block = block
      end

      def !@
        RejectCondition.new(@column, @block)
      end

      def evaluate
        BooleanArray.new(@column.collect(&@block))
      end
    end

    class RejectCondition < Condition
      def initialize(column, block)
        @column = column
        @block = block
      end

      def !@
        SelectCondition.new(@column, @block)
      end

      def evaluate
        raw_array = @column.collect do |value|
          evaluated_value = @block.call(value)
          if evaluated_value.nil?
            nil
          else
            not evaluated_value
          end
        end
        BooleanArray.new(raw_array)
      end
    end
  end
end
