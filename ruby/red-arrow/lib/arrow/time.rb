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
  class Time
    attr_reader :unit
    attr_reader :value
    def initialize(unit, value)
      @unit = unit
      @value = value
      @unconstructed = false
    end

    def ==(other)
      other.is_a?(self.class) and
        positive? == other.positive? and
        hour == other.hour and
        minute == other.minute and
        second == other.second and
        nano_second == other.nano_second
    end

    def cast(target_unit)
      return self.class.new(@unit, @value) if @unit == target_unit

      target_value = (hour * 60 * 60) + (minute * 60) + second
      case target_unit
      when TimeUnit::MILLI
        target_value *= 1000
        target_value += nano_second / 1000 / 1000
      when TimeUnit::MICRO
        target_value *= 1000 * 1000
        target_value += nano_second / 1000
      when TimeUnit::NANO
        target_value *= 1000 * 1000 * 1000
        target_value += nano_second
      end
      target_value = -target_value if negative?
      self.class.new(target_unit, target_value)
    end

    def to_f
      case @unit
      when TimeUnit::SECOND
        @value.to_f
      when TimeUnit::MILLI
        @value.to_f / 1000.0
      when TimeUnit::MICRO
        @value.to_f / 1000.0 / 1000.0
      when TimeUnit::NANO
        @value.to_f / 1000.0 / 1000.0 / 1000.0
      end
    end

    def positive?
      @value.positive?
    end

    def negative?
      @value.negative?
    end

    def hour
      unconstruct
      @hour
    end

    def minute
      unconstruct
      @minute
    end
    alias_method :min, :minute

    def second
      unconstruct
      @second
    end
    alias_method :sec, :second

    def nano_second
      unconstruct
      @nano_second
    end
    alias_method :nsec, :nano_second

    def to_s
      unconstruct
      if @nano_second.zero?
        nano_second_string = ""
      else
        nano_second_string = (".%09d" % @nano_second).gsub(/0+\z/, "")
      end
      "%s%02d:%02d:%02d%s" % [
        @value.negative? ? "-" : "",
        @hour,
        @minute,
        @second,
        nano_second_string,
      ]
    end

    private
    def unconstruct
      return if @unconstructed
      abs_value = @value.abs
      case unit
      when TimeUnit::SECOND
        unconstruct_second(abs_value)
        @nano_second = 0
      when TimeUnit::MILLI
        unconstruct_second(abs_value / 1000)
        @nano_second = (abs_value % 1000) * 1000 * 1000
      when TimeUnit::MICRO
        unconstruct_second(abs_value / 1000 / 1000)
        @nano_second = (abs_value % (1000 * 1000)) * 1000
      when TimeUnit::NANO
        unconstruct_second(abs_value / 1000 / 1000 / 1000)
        @nano_second = abs_value % (1000 * 1000 * 1000)
      else
        raise ArgumentError, "invalid unit: #{@unit.inspect}"
      end
      @unconstructed = true
    end

    def unconstruct_second(abs_value_in_second)
      if abs_value_in_second < 60
        hour = 0
        minute = 0
        second = abs_value_in_second
      elsif abs_value_in_second < (60 * 60)
        hour = 0
        minute = abs_value_in_second / 60
        second = abs_value_in_second % 60
      else
        in_minute = abs_value_in_second / 60
        hour = in_minute / 60
        minute = in_minute % 60
        second = abs_value_in_second % 60
      end
      @hour = hour
      @minute = minute
      @second = second
    end
  end
end
