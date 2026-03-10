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

require_relative "array"
require_relative "field"
require_relative "flatbuffers"
require_relative "record-batch"
require_relative "schema"
require_relative "type"

module ArrowFormat
  module Readable
    private
    def read_custom_metadata(fb_custom_metadata)
      return nil if fb_custom_metadata.nil?
      metadata = {}
      fb_custom_metadata.each do |key_value|
        metadata[key_value.key] = key_value.value
      end
      metadata
    end

    def read_schema(fb_schema)
      fields = fb_schema.fields.collect do |fb_field|
        read_field(fb_field)
      end
      Schema.new(fields,
                 metadata: read_custom_metadata(fb_schema.custom_metadata))
    end

    def read_field(fb_field,
                   map_entries: false,
                   map_key: false,
                   map_value: false)
      fb_type = fb_field.type
      case fb_type
      when FB::Null
        type = NullType.singleton
      when FB::Bool
        type = BooleanType.singleton
      when FB::Int
        type = read_type_int(fb_type)
      when FB::FloatingPoint
        case fb_type.precision
        when FB::Precision::SINGLE
          type = Float32Type.singleton
        when FB::Precision::DOUBLE
          type = Float64Type.singleton
        else
          raise ReadError.new("Unsupported type: #{fb_type.inspect}")
        end
      when FB::Date
        case fb_type.unit
        when FB::DateUnit::DAY
          type = Date32Type.singleton
        when FB::DateUnit::MILLISECOND
          type = Date64Type.singleton
        else
          raise ReadError.new("Unsupported type: #{fb_type.inspect}")
        end
      when FB::Time
        case fb_type.bit_width
        when 32
          case fb_type.unit
          when FB::TimeUnit::SECOND
            type = Time32Type.new(:second)
          when FB::TimeUnit::MILLISECOND
            type = Time32Type.new(:millisecond)
          else
            raise ReadError.new("Unsupported type: #{fb_type.inspect}")
          end
        when 64
          case fb_type.unit
          when FB::TimeUnit::MICROSECOND
            type = Time64Type.new(:microsecond)
          when FB::TimeUnit::NANOSECOND
            type = Time64Type.new(:nanosecond)
          else
            raise ReadError.new("Unsupported type: #{fb_type.inspect}")
          end
        end
      when FB::Timestamp
        unit = fb_type.unit.name.downcase.to_sym
        type = TimestampType.new(unit, fb_type.timezone)
      when FB::Interval
        case fb_type.unit
        when FB::IntervalUnit::YEAR_MONTH
          type = YearMonthIntervalType.singleton
        when FB::IntervalUnit::DAY_TIME
          type = DayTimeIntervalType.singleton
        when FB::IntervalUnit::MONTH_DAY_NANO
          type = MonthDayNanoIntervalType.singleton
        else
          raise ReadError.new("Unsupported type: #{fb_type.inspect}")
        end
      when FB::Duration
        unit = fb_type.unit.name.downcase.to_sym
        type = DurationType.new(unit)
      when FB::List
        type = ListType.new(read_field(fb_field.children[0]))
      when FB::LargeList
        type = LargeListType.new(read_field(fb_field.children[0]))
      when FB::FixedSizeList
        type = FixedSizeListType.new(read_field(fb_field.children[0]),
                                     fb_type.list_size)
      when FB::Struct
        if map_entries
          fb_children = fb_field.children
          children = [
            read_field(fb_children[0], map_key: true),
            read_field(fb_children[1], map_value: true),
          ]
        else
          children = fb_field.children.collect {|child| read_field(child)}
        end
        type = StructType.new(children)
      when FB::Union
        children = fb_field.children.collect {|child| read_field(child)}
        type_ids = fb_type.type_ids
        case fb_type.mode
        when FB::UnionMode::DENSE
          type = DenseUnionType.new(children, type_ids)
        when FB::UnionMode::SPARSE
          type = SparseUnionType.new(children, type_ids)
        else
          raise ReadError.new("Unsupported type: #{fb_type.inspect}")
        end
      when FB::Map
        type = MapType.new(read_field(fb_field.children[0], map_entries: true),
                           fb_type.keys_sorted?)
      when FB::Binary
        type = BinaryType.singleton
      when FB::LargeBinary
        type = LargeBinaryType.singleton
      when FB::Utf8
        type = UTF8Type.singleton
      when FB::LargeUtf8
        type = LargeUTF8Type.singleton
      when FB::FixedSizeBinary
        type = FixedSizeBinaryType.new(fb_type.byte_width)
      when FB::Decimal
        case fb_type.bit_width
        when 128
          type = Decimal128Type.new(fb_type.precision, fb_type.scale)
        when 256
          type = Decimal256Type.new(fb_type.precision, fb_type.scale)
        else
          raise ReadError.new("Unsupported type: #{fb_type.inspect}")
        end
      else
        raise ReadError.new("Unsupported type: #{fb_type.inspect}")
      end

      dictionary = fb_field.dictionary
      if dictionary
        dictionary_id = dictionary.id
        index_type = read_type_int(dictionary.index_type)
        value_type = type
        type = DictionaryType.new(dictionary_id,
                                  index_type,
                                  value_type,
                                  dictionary.ordered?)
      end

      # Map type uses static "entries"/"key"/"value" as field names
      # instead of field names in FlatBuffers. It's based on the
      # specification:
      #
      #   The names of the child fields may be respectively "entries",
      #   "key", and "value", but this is not enforced.
      if map_entries
        name = "entries"
      elsif map_key
        name = "key"
      elsif map_value
        name = "value"
      else
        name = fb_field.name
      end
      Field.new(name,
                type,
                nullable: fb_field.nullable?,
                metadata: read_custom_metadata(fb_field.custom_metadata))
    end

    def read_type_int(fb_type)
      case fb_type.bit_width
      when 8
        if fb_type.signed?
          Int8Type.singleton
        else
          UInt8Type.singleton
        end
      when 16
        if fb_type.signed?
          Int16Type.singleton
        else
          UInt16Type.singleton
        end
      when 32
        if fb_type.signed?
          Int32Type.singleton
        else
          UInt32Type.singleton
        end
      when 64
        if fb_type.signed?
          Int64Type.singleton
        else
          UInt64Type.singleton
        end
      end
    end

    def read_record_batch(version, fb_record_batch, schema, body)
      n_rows = fb_record_batch.length
      nodes = fb_record_batch.nodes
      buffers = fb_record_batch.buffers
      columns = schema.fields.collect do |field|
        read_column(version, field, nodes, buffers, body)
      end
      RecordBatch.new(schema, n_rows, columns)
    end

    def read_column(version, field, nodes, buffers, body)
      node = nodes.shift
      length = node.length

      return field.type.build_array(length) if field.type.is_a?(NullType)

      validity_buffer = buffers.shift
      if validity_buffer.length.zero?
        validity = nil
      else
        validity = body.slice(validity_buffer.offset, validity_buffer.length)
      end

      case field.type
      when BooleanType,
           NumberType,
           TemporalType
        values_buffer = buffers.shift
        values = body&.slice(values_buffer.offset, values_buffer.length)
        field.type.build_array(length, validity, values)
      when VariableSizeBinaryType
        offsets_buffer = buffers.shift
        values_buffer = buffers.shift
        offsets = body&.slice(offsets_buffer.offset, offsets_buffer.length)
        values = body&.slice(values_buffer.offset, values_buffer.length)
        field.type.build_array(length, validity, offsets, values)
      when FixedSizeBinaryType
        values_buffer = buffers.shift
        values = body&.slice(values_buffer.offset, values_buffer.length)
        field.type.build_array(length, validity, values)
      when VariableSizeListType
        offsets_buffer = buffers.shift
        offsets = body&.slice(offsets_buffer.offset, offsets_buffer.length)
        child = read_column(version, field.type.child, nodes, buffers, body)
        field.type.build_array(length, validity, offsets, child)
      when FixedSizeListType
        child = read_column(version, field.type.child, nodes, buffers, body)
        field.type.build_array(length, validity, child)
      when StructType
        children = field.type.children.collect do |child|
          read_column(version, child, nodes, buffers, body)
        end
        field.type.build_array(length, validity, children)
      when DenseUnionType
        if version == FB::MetadataVersion::V4
          # Dense union type has validity with V4.
          types_buffer = buffers.shift
          types = body&.slice(types_buffer.offset, types_buffer.length)
        else
          # Dense union type doesn't have validity.
          types = validity
        end
        offsets_buffer = buffers.shift
        offsets = body&.slice(offsets_buffer.offset, offsets_buffer.length)
        children = field.type.children.collect do |child|
          read_column(version, child, nodes, buffers, body)
        end
        field.type.build_array(length, types, offsets, children)
      when SparseUnionType
        if version == FB::MetadataVersion::V4
          # Sparse union type has validity with V4.
          types_buffer = buffers.shift
          types = body&.slice(types_buffer.offset, types_buffer.length)
        else
          # Sparse union type doesn't have validity.
          types = validity
        end
        children = field.type.children.collect do |child|
          read_column(version, child, nodes, buffers, body)
        end
        field.type.build_array(length, types, children)
      when DictionaryType
        indices_buffer = buffers.shift
        indices = body&.slice(indices_buffer.offset, indices_buffer.length)
        dictionaries = find_dictionaries(field.type.id)
        field.type.build_array(length, validity, indices, dictionaries)
      end
    end
  end
end
