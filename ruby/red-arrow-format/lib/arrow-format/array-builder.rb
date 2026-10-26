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

module ArrowFormat
  class ArrayBuilder
    def initialize(values)
      @values = values
    end

    def build
      target = nil
      @values.each do |value|
        target = detect_target(value, target)
        break if target and target[:detected]
      end
      if target
        array_class = target[:array_class]
        array_class ||= finalize_array_class(target)
        type = target[:type]
        if type
          array_class.new(type, @values)
        else
          array_class.new(@values)
        end
      else
        UTF8Array.new(@values)
      end
    end

    private
    def detect_target(value, target)
      case value
      when nil
        target
      when true, false
        {
          array_class: BooleanArray,
          detected: true,
        }
      when String
        {
          array_class: UTF8Array,
          detected: true,
        }
      when Float
        {
          array_class: Float64Array,
          detected: true,
        }
      when Integer
        target ||= {}
        min = target[:min] || value
        max = target[:max] || value
        min = value if value < min
        max = value if value > max

        if target[:value_type] == :int or value < 0
          {
            value_type: :int,
            min: min,
            max: max,
          }
        else
          {
            value_type: :uint,
            min: min,
            max: max,
          }
        end
      when Time
        {
          array_class: TimestampArray,
          type: TimestampType.new(:nanosecond),
          detected: true,
        }
      when Date
        {
          array_class: Date32Array,
          detected: true,
        }
      else
        {
          array_class: UTF8Array,
          detected: true,
        }
      end
    end

    def finalize_array_class(target)
      value_type = target[:value_type]
      case value_type
      when :int
        min = target[:min]
        max = target[:max]

        if Int8Type::MIN_VALUE <= min and max <= Int8Type::MAX_VALUE
          Int8Array
        elsif Int16Type::MIN_VALUE <= min and max <= Int16Type::MAX_VALUE
          Int16Array
        elsif Int32Type::MIN_VALUE <= min and max <= Int32Type::MAX_VALUE
          Int32Array
        elsif Int64Type::MIN_VALUE <= min and max <= Int64Type::MAX_VALUE
          Int64Array
        else
          UTF8Array
        end
      when :uint
        max = target[:max]

        if max <= UInt8Type::MAX_VALUE
          UInt8Array
        elsif max <= UInt16Type::MAX_VALUE
          UInt16Array
        elsif max <= UInt32Type::MAX_VALUE
          UInt32Array
        elsif max <= UInt64Type::MAX_VALUE
          UInt64Array
        else
          UTF8Array
        end
      else
        UTF8Array
      end
    end
  end
end
