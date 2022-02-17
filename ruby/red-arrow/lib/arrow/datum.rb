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
  class Datum
    class << self
      # @api private
      def try_convert(value)
        case value
        when Table
          TableDatum.new(value)
        when Array
          ArrayDatum.new(value)
        when ChunkedArray
          ChunkedArrayDatum.new(value)
        when Column
          ChunkedArrayDatum.new(value.data)
        when Scalar
          ScalarDatum.new(value)
        when ::Array
          ArrayDatum.new(ArrayBuilder.build(value))
        when Integer
          case value
          when (0..((2 ** 8) - 1))
            try_convert(UInt8Scalar.new(value))
          when ((-(2 ** 7))..((2 ** 7) - 1))
            try_convert(Int8Scalar.new(value))
          when (0..((2 ** 16) - 1))
            try_convert(UInt16Scalar.new(value))
          when ((-(2 ** 15))..((2 ** 15) - 1))
            try_convert(Int16Scalar.new(value))
          when (0..((2 ** 32) - 1))
            try_convert(UInt32Scalar.new(value))
          when ((-(2 ** 31))..((2 ** 31) - 1))
            try_convert(Int32Scalar.new(value))
          when (0..((2 ** 64) - 1))
            try_convert(UInt64Scalar.new(value))
          when ((-(2 ** 63))..((2 ** 63) - 1))
            try_convert(Int64Scalar.new(value))
          else
            nil
          end
        when Float
          try_convert(DoubleScalar.new(value))
        when true, false
          try_convert(BooleanScalar.new(value))
        when String
          if value.ascii_only? or value.encoding == Encoding::UTF_8
            if value.bytesize <= ((2 ** 31) - 1)
              try_convert(StringScalar.new(value))
            else
              try_convert(LargeStringScalar.new(value))
            end
          else
            if value.bytesize <= ((2 ** 31) - 1)
              try_convert(BinaryScalar.new(value))
            else
              try_convert(LargeBinaryScalar.new(value))
            end
          end
        when Date
          date32_value = (value - Date32ArrayBuilder::UNIX_EPOCH).to_i
          try_convert(Date32Scalar.new(date32_value))
        when Time
          case value.unit
          when TimeUnit::SECOND, TimeUnit::MILLI
            data_type = Time32DataType.new(value.unit)
            scalar_class = Time32Scalar
          else
            data_type = Time64DataType.new(value.unit)
            scalar_class = Time64Scalar
          end
          try_convert(scalar_class.new(data_type, value.value))
        when ::Time
          data_type = TimestampDataType.new(:nano)
          timestamp_value = value.to_i * 1_000_000_000 + value.nsec
          try_convert(TimestampScalar.new(data_type, timestamp_value))
        when Decimal128
          data_type = TimestampDataType.new(:nano)
          timestamp_value = value.to_i * 1_000_000_000 + value.nsec
          try_convert(Decimal128Scalar.new(data_type, timestamp_value))
        else
          nil
        end
      end
    end
  end
end
