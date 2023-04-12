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

    module Helper
      class << self
        def ensure_boolean(column)
          case column.data_type
          when Arrow::BooleanDataType
            column.data
          else
            options = CastOptions.new
            options.to_data_type = Arrow::BooleanDataType.new
            Function.find("cast").execute([column.data], options).value
          end
        end
      end
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
        function.execute([@condition1.evaluate, @condition2.evaluate]).value
      end
    end

    class AndCondition < LogicalCondition
      private
      def function
        Function.find("and")
      end
    end

    class OrCondition < LogicalCondition
      private
      def function
        Function.find("or")
      end
    end

    class XorCondition < LogicalCondition
      private
      def function
        Function.find("xor")
      end
    end

    class ColumnCondition < Condition
      def initialize(column)
        @column = column
      end

      def evaluate
        Helper.ensure_boolean(@column)
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

      def end_with?(substring, ignore_case: false)
        MatchSubstringFamilyCondition.new("ends_with",
                                          @column, substring, ignore_case)
      end

      def match_like?(pattern, ignore_case: false)
        MatchSubstringFamilyCondition.new("match_like",
                                          @column, pattern, ignore_case)
      end

      def match_substring?(pattern, ignore_case: nil)
        case pattern
        when String
          ignore_case = false if ignore_case.nil?
          MatchSubstringFamilyCondition.new("match_substring",
                                            @column, pattern, ignore_case)
        when Regexp
          ignore_case = pattern.casefold? if ignore_case.nil?
          MatchSubstringFamilyCondition.new("match_substring_regex",
                                            @column,
                                            pattern.source,
                                            ignore_case)
        else
          message =
             "pattern must be either String or Regexp: #{pattern.inspect}"
          raise ArgumentError, message
        end 
      end

      def start_with?(substring, ignore_case: false)
        MatchSubstringFamilyCondition.new("starts_with",
                                          @column, substring, ignore_case)
      end
    end

    class NotColumnCondition < Condition
      def initialize(column)
        @column = column
      end

      def evaluate
        data = Helper.ensure_boolean(@column)
        Function.find("invert").execute([data]).value
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
        if @value.nil?
          Function.find("is_null").execute([@column.data]).value
        else
          Function.find("equal").execute([@column.data, @value]).value
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
        if @value.nil?
          Function.find("is_valid").execute([@column.data]).value
        else
          Function.find("not_equal").execute([@column.data, @value]).value
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
        Function.find("less").execute([@column.data, @value]).value
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
        Function.find("less_equal").execute([@column.data, @value]).value
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
        Function.find("greater").execute([@column.data, @value]).value
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
        Function.find("greater_equal").execute([@column.data, @value]).value
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
        values = @values
        values = Array.new(values) unless values.is_a?(Array)
        options = SetLookupOptions.new(values)
        Function.find("is_in").execute([@column.data], options).value
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
        values = @values
        values = Array.new(values) unless values.is_a?(Array)
        options = SetLookupOptions.new(values)
        booleans = Function.find("is_in").execute([@column.data], options).value
        Function.find("invert").execute([booleans]).value
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

    class MatchSubstringFamilyCondition < Condition
      def initialize(function, column, pattern, ignore_case, invert: false)
        @function = function
        @column = column
        @options = MatchSubstringOptions.new
        @options.pattern = pattern
        @options.ignore_case = ignore_case
        @invert = invert
      end

      def !@
        MatchSubstringFamilyCondition.new(@function,
                                          @column,
                                          @options.pattern,
                                          @options.ignore_case?,
                                          invert: !@invert)
      end

      def evaluate
        datum = Function.find(@function).execute([@column.data], @options)
        if @invert
          datum = Function.find("invert").execute([datum])
        end
        datum.value
      end
    end
  end
end
