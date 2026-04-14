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

require "json"

module ArrowFormat
  module Integration
    class JSONReader
      attr_reader :schema
      def initialize(input)
        @json = JSON.load(input.read)
        @dictionaries = {}
        @schema = read_schema
      end

      def each
        @json["batches"].each do |record_batch|
          yield(read_record_batch(record_batch))
        end
      end

      private
      def read_metadata(metadata)
        return nil if metadata.nil?

        metadata.inject({}) do |result, metadatum|
          result[metadatum["key"]] = metadatum["value"]
          result
        end
      end

      def read_type(type, children)
        case type["name"]
        when "null"
          NullType.singleton
        when "bool"
          BooleanType.singleton
        when "int"
          is_signed = type["isSigned"]
          case type["bitWidth"]
          when 8
            if is_signed
              Int8Type.singleton
            else
              UInt8Type.singleton
            end
          when 16
            if is_signed
              Int16Type.singleton
            else
              UInt16Type.singleton
            end
          when 32
            if is_signed
              Int32Type.singleton
            else
              UInt32Type.singleton
            end
          when 64
            if is_signed
              Int64Type.singleton
            else
              UInt64Type.singleton
            end
          else
            raise "Unsupported type: #{type.inspect}: #{children.inspect}"
          end
        when "floatingpoint"
          case type["precision"]
          when "SINGLE"
            Float32Type.singleton
          when "DOUBLE"
            Float64Type.singleton
          else
            raise "Unsupported type: #{type.inspect}: #{children.inspect}"
          end
        when "date"
          case type["unit"]
          when "DAY"
            Date32Type.singleton
          when "MILLISECOND"
            Date64Type.singleton
          else
            raise "Unsupported type: #{type.inspect}: #{children.inspect}"
          end
        when "time"
          unit = type["unit"].downcase.to_sym
          case type["bitWidth"]
          when 32
            Time32Type.new(unit)
          when 64
            Time64Type.new(unit)
          else
            raise "Unsupported type: #{type.inspect}: #{children.inspect}"
          end
        when "timestamp"
          unit = type["unit"].downcase.to_sym
          TimestampType.new(unit, type["timezone"])
        when "interval"
          case type["unit"]
          when "YEAR_MONTH"
            YearMonthIntervalType.singleton
          when "DAY_TIME"
            DayTimeIntervalType.singleton
          when "MONTH_DAY_NANO"
            MonthDayNanoIntervalType.singleton
          else
            raise "Unsupported type: #{type.inspect}: #{children.inspect}"
          end
        when "duration"
          DurationType.new(type["unit"].downcase.to_sym)
        when "binary"
          BinaryType.singleton
        when "largebinary"
          LargeBinaryType.singleton
        when "utf8"
          UTF8Type.singleton
        when "largeutf8"
          LargeUTF8Type.singleton
        when "fixedsizebinary"
          FixedSizeBinaryType.new(type["byteWidth"])
        when "decimal"
          precision = type["precision"]
          scale = type["scale"]
          case type["bitWidth"]
          when 128
            Decimal128Type.new(precision, scale)
          when 256
            Decimal256Type.new(precision, scale)
          else
            raise "Unsupported type: #{type.inspect}: #{children.inspect}"
          end
        when "list"
          ListType.new(read_field(children[0]))
        when "largelist"
          LargeListType.new(read_field(children[0]))
        when "fixedsizelist"
          FixedSizeListType.new(read_field(children[0]), type["listSize"])
        when "struct"
          StructType.new(children.collect {|child| read_field(child)})
        when "map"
          MapType.new(read_field(children[0]), type["keysSorted"])
        when "union"
          children = children.collect {|child| read_field(child)}
          type_ids = type["typeIds"]
          case type["mode"]
          when "DENSE"
            DenseUnionType.new(children, type_ids)
          when "SPARSE"
            SparseUnionType.new(children, type_ids)
          else
            raise "Unsupported type: #{type.inspect}: #{children.inspect}"
          end
        else
          raise "Unsupported type: #{type.inspect}: #{children.inspect}"
        end
      end

      def read_field(field)
        type = read_type(field["type"], field["children"])
        dictionary = field["dictionary"]
        if dictionary
          index_type = read_type(dictionary["indexType"], [])
          value_type = type
          type = DictionaryType.new(dictionary["id"],
                                    index_type,
                                    value_type,
                                    dictionary["isOrdered"])
        end
        metadata = read_metadata(field["metadata"])
        Field.new(field["name"],
                  type,
                  nullable: field["nullable"],
                  metadata: metadata)
      end

      def read_dictionary(id, type)
        @json["dictionaries"].each do |dictionary|
          next unless dictionary["id"] == id
          return read_array(dictionary["data"]["columns"][0], type)
        end
      end

      def read_schema
        fields = []
        @json["schema"]["fields"].each do |field|
          fields << read_field(field)
        end
        metadata = read_metadata(@json["schema"]["metadata"])
        Schema.new(fields, metadata: metadata)
      end

      def read_bitmap(bitmap)
        buffer = +"".b
        bitmap.each_slice(8) do |bits|
          byte = 0
          while bits.size < 8
            bits << 0
          end
          bits.reverse_each do |bit|
            byte = (byte << 1) + bit
          end
          buffer << [byte].pack("C")
        end
        IO::Buffer.for(buffer)
      end

      def read_types(types)
        buffer_type = :S8
        size = IO::Buffer.size_of(buffer_type)
        buffer = IO::Buffer.new(size * types.size)
        types.each_with_index do |type, i|
          offset = size * i
          buffer.set_value(buffer_type, offset, type)
        end
        buffer
      end

      def read_offsets(offsets, type)
        return nil if offsets.nil?

        case type
        when LargeListType, LargeBinaryType, LargeUTF8Type
          offsets = offsets.collect {|offset| Integer(offset, 10)}
        end
        size = IO::Buffer.size_of(type.offset_buffer_type)
        buffer = IO::Buffer.new(size * offsets.size)
        offsets.each_with_index do |offset, i|
          value_offset = size * i
          buffer.set_value(type.offset_buffer_type, value_offset, offset)
        end
        buffer
      end

      def read_hex_value(value)
        values = value.scan(/.{2}/).collect do |hex|
          Integer(hex, 16)
        end
        values.pack("C*")
      end

      def read_values(data, type)
        case type
        when BooleanType
          read_bitmap(data.collect {|boolean| boolean ? 1 : 0})
        when DayTimeIntervalType
          buffer_types = [type.buffer_type] * 2
          size = IO::Buffer.size_of(buffer_types)
          buffer = IO::Buffer.new(size * data.size)
          data.each_with_index do |value, i|
            offset = size * i
            components = value.fetch_values("days", "milliseconds")
            buffer.set_values(buffer_types, offset, components)
          end
          buffer
        when MonthDayNanoIntervalType
          size = IO::Buffer.size_of(type.buffer_types)
          buffer = IO::Buffer.new(size * data.size)
          data.each_with_index do |value, i|
            offset = size * i
            components = value.fetch_values("months", "days", "nanoseconds")
            buffer.set_values(type.buffer_types, offset, components)
          end
          buffer
        when NumberType,
             TemporalType
          size = IO::Buffer.size_of(type.buffer_type)
          buffer = IO::Buffer.new(size * data.size)
          data.each_with_index do |value, i|
            offset = size * i
            # If the type is 64bit such as `Int64Type`, `value` is a
            # string not integer to round-trip data through JSON.
            value = Integer(value, 10) if value.is_a?(String)
            buffer.set_value(type.buffer_type, offset, value)
          end
          buffer
        when DecimalType
          byte_width = type.byte_width
          bit_width = byte_width * 8
          buffer = IO::Buffer.new(byte_width * data.size)
          data.each_with_index do |value, i|
            offset = byte_width * i
            components = []
            value = BigDecimal(value)
            value *= 10 ** value.scale if value.scale > 0
            bits = "%0#{bit_width}b" % value.to_i
            if value.negative?
              # `bits` starts with "..1".
              #
              # If `value` is the minimum negative value, `bits` may
              # be larger than `bit_width` because of the start `..`
              # (2 characters).
              bits = bits.delete_prefix("..").rjust(bit_width, "1")
            end
            bits.scan(/[01]{64}/).reverse_each do |chunk|
              buffer.set_value(:u64, offset, Integer(chunk, 2))
              offset += 8
            end
          end
          buffer
        when UTF8Type, LargeUTF8Type
          IO::Buffer.for(data.join)
        when VariableSizeBinaryType, FixedSizeBinaryType
          IO::Buffer.for(data.collect {|value| read_hex_value(value)}.join)
        else
          raise "Unsupported values: #{data.inspect}: #{type.inspect}"
        end
      end

      def read_array(column, type)
        length = column["count"]
        case type
        when NullType
          type.build_array(length)
        when BooleanType,
             NumberType,
             TemporalType,
             FixedSizeBinaryType
          validity_buffer = read_bitmap(column["VALIDITY"])
          values_buffer = read_values(column["DATA"], type)
          type.build_array(length, validity_buffer, values_buffer)
        when VariableSizeBinaryType
          validity_buffer = read_bitmap(column["VALIDITY"])
          offsets_buffer = read_offsets(column["OFFSET"], type)
          values_buffer = read_values(column["DATA"], type)
          type.build_array(length,
                           validity_buffer,
                           offsets_buffer,
                           values_buffer)
        when VariableSizeListType
          validity_buffer = read_bitmap(column["VALIDITY"])
          offsets_buffer = read_offsets(column["OFFSET"], type)
          child = read_array(column["children"][0], type.child.type)
          type.build_array(length,
                           validity_buffer,
                           offsets_buffer,
                           child)
        when FixedSizeListType
          validity_buffer = read_bitmap(column["VALIDITY"])
          child = read_array(column["children"][0], type.child.type)
          type.build_array(length, validity_buffer, child)
        when StructType
          validity_buffer = read_bitmap(column["VALIDITY"])
          children = column["children"]
                       .zip(type.children)
                       .collect do |child_column, child_field|
            read_array(child_column, child_field.type)
          end
          type.build_array(length, validity_buffer, children)
        when DenseUnionType
          types_buffer = read_types(column["TYPE_ID"])
          offsets_buffer = read_offsets(column["OFFSET"], type)
          children = column["children"]
                       .zip(type.children)
                       .collect do |child_column, child_field|
            read_array(child_column, child_field.type)
          end
          type.build_array(length,
                           types_buffer,
                           offsets_buffer,
                           children)
        when SparseUnionType
          types_buffer = read_types(column["TYPE_ID"])
          children = column["children"]
                       .zip(type.children)
                       .collect do |child_column, child_field|
            read_array(child_column, child_field.type)
          end
          type.build_array(length, types_buffer, children)
        when DictionaryType
          validity_buffer = read_bitmap(column["VALIDITY"])
          indices_buffer = read_values(column["DATA"], type.index_type)
          dictionary_array = read_dictionary(type.id, type.value_type)
          dictionary = Dictionary.new(type.id, dictionary_array)
          type.build_array(length,
                           validity_buffer,
                           indices_buffer,
                           [dictionary])
        else
          raise "Unsupported array: #{column.inspect}: #{field.inspect}"
        end
      end

      def read_record_batch(record_batch)
        n_rows = record_batch["count"]
        columns = record_batch["columns"]
                    .zip(@schema.fields)
                    .collect do |column, field|
          read_array(column, field.type)
        end
        RecordBatch.new(@schema, n_rows, columns)
      end
    end
  end
end
