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
require_relative "record-batch"
require_relative "schema"
require_relative "type"

require_relative "org/apache/arrow/flatbuf/binary"
require_relative "org/apache/arrow/flatbuf/bool"
require_relative "org/apache/arrow/flatbuf/date"
require_relative "org/apache/arrow/flatbuf/date_unit"
require_relative "org/apache/arrow/flatbuf/decimal"
require_relative "org/apache/arrow/flatbuf/dictionary_encoding"
require_relative "org/apache/arrow/flatbuf/dictionary_batch"
require_relative "org/apache/arrow/flatbuf/duration"
require_relative "org/apache/arrow/flatbuf/fixed_size_binary"
require_relative "org/apache/arrow/flatbuf/floating_point"
require_relative "org/apache/arrow/flatbuf/int"
require_relative "org/apache/arrow/flatbuf/interval"
require_relative "org/apache/arrow/flatbuf/interval_unit"
require_relative "org/apache/arrow/flatbuf/large_binary"
require_relative "org/apache/arrow/flatbuf/large_list"
require_relative "org/apache/arrow/flatbuf/large_utf8"
require_relative "org/apache/arrow/flatbuf/list"
require_relative "org/apache/arrow/flatbuf/map"
require_relative "org/apache/arrow/flatbuf/message"
require_relative "org/apache/arrow/flatbuf/null"
require_relative "org/apache/arrow/flatbuf/precision"
require_relative "org/apache/arrow/flatbuf/record_batch"
require_relative "org/apache/arrow/flatbuf/schema"
require_relative "org/apache/arrow/flatbuf/struct_"
require_relative "org/apache/arrow/flatbuf/time"
require_relative "org/apache/arrow/flatbuf/time_unit"
require_relative "org/apache/arrow/flatbuf/timestamp"
require_relative "org/apache/arrow/flatbuf/union"
require_relative "org/apache/arrow/flatbuf/union_mode"
require_relative "org/apache/arrow/flatbuf/utf8"

module ArrowFormat
  module Readable
    private
    def read_schema(fb_schema)
      fields = fb_schema.fields.collect do |fb_field|
        read_field(fb_field)
      end
      Schema.new(fields)
    end

    def read_field(fb_field)
      fb_type = fb_field.type
      case fb_type
      when Org::Apache::Arrow::Flatbuf::Null
        type = NullType.singleton
      when Org::Apache::Arrow::Flatbuf::Bool
        type = BooleanType.singleton
      when Org::Apache::Arrow::Flatbuf::Int
        type = read_type_int(fb_type)
      when Org::Apache::Arrow::Flatbuf::FloatingPoint
        case fb_type.precision
        when Org::Apache::Arrow::Flatbuf::Precision::SINGLE
          type = Float32Type.singleton
        when Org::Apache::Arrow::Flatbuf::Precision::DOUBLE
          type = Float64Type.singleton
        end
      when Org::Apache::Arrow::Flatbuf::Date
        case fb_type.unit
        when Org::Apache::Arrow::Flatbuf::DateUnit::DAY
          type = Date32Type.singleton
        when Org::Apache::Arrow::Flatbuf::DateUnit::MILLISECOND
          type = Date64Type.singleton
        end
      when Org::Apache::Arrow::Flatbuf::Time
        case fb_type.bit_width
        when 32
          case fb_type.unit
          when Org::Apache::Arrow::Flatbuf::TimeUnit::SECOND
            type = Time32Type.new(:second)
          when Org::Apache::Arrow::Flatbuf::TimeUnit::MILLISECOND
            type = Time32Type.new(:millisecond)
          end
        when 64
          case fb_type.unit
          when Org::Apache::Arrow::Flatbuf::TimeUnit::MICROSECOND
            type = Time64Type.new(:microsecond)
          when Org::Apache::Arrow::Flatbuf::TimeUnit::NANOSECOND
            type = Time64Type.new(:nanosecond)
          end
        end
      when Org::Apache::Arrow::Flatbuf::Timestamp
        unit = fb_type.unit.name.downcase.to_sym
        type = TimestampType.new(unit, fb_type.timezone)
      when Org::Apache::Arrow::Flatbuf::Interval
        case fb_type.unit
        when Org::Apache::Arrow::Flatbuf::IntervalUnit::YEAR_MONTH
          type = YearMonthIntervalType.new
        when Org::Apache::Arrow::Flatbuf::IntervalUnit::DAY_TIME
          type = DayTimeIntervalType.new
        when Org::Apache::Arrow::Flatbuf::IntervalUnit::MONTH_DAY_NANO
          type = MonthDayNanoIntervalType.new
        end
      when Org::Apache::Arrow::Flatbuf::Duration
        unit = fb_type.unit.name.downcase.to_sym
        type = DurationType.new(unit)
      when Org::Apache::Arrow::Flatbuf::List
        type = ListType.new(read_field(fb_field.children[0]))
      when Org::Apache::Arrow::Flatbuf::LargeList
        type = LargeListType.new(read_field(fb_field.children[0]))
      when Org::Apache::Arrow::Flatbuf::Struct
        children = fb_field.children.collect {|child| read_field(child)}
        type = StructType.new(children)
      when Org::Apache::Arrow::Flatbuf::Union
        children = fb_field.children.collect {|child| read_field(child)}
        type_ids = fb_type.type_ids
        case fb_type.mode
        when Org::Apache::Arrow::Flatbuf::UnionMode::DENSE
          type = DenseUnionType.new(children, type_ids)
        when Org::Apache::Arrow::Flatbuf::UnionMode::SPARSE
          type = SparseUnionType.new(children, type_ids)
        end
      when Org::Apache::Arrow::Flatbuf::Map
        type = MapType.new(read_field(fb_field.children[0]))
      when Org::Apache::Arrow::Flatbuf::Binary
        type = BinaryType.singleton
      when Org::Apache::Arrow::Flatbuf::LargeBinary
        type = LargeBinaryType.singleton
      when Org::Apache::Arrow::Flatbuf::Utf8
        type = UTF8Type.singleton
      when Org::Apache::Arrow::Flatbuf::LargeUtf8
        type = LargeUTF8Type.singleton
      when Org::Apache::Arrow::Flatbuf::FixedSizeBinary
        type = FixedSizeBinaryType.new(fb_type.byte_width)
      when Org::Apache::Arrow::Flatbuf::Decimal
        case fb_type.bit_width
        when 128
          type = Decimal128Type.new(fb_type.precision, fb_type.scale)
        end
      end

      dictionary = fb_field.dictionary
      if dictionary
        dictionary_id = dictionary.id
        index_type = read_type_int(dictionary.index_type)
        type = DictionaryType.new(index_type, type, dictionary.ordered?)
      else
        dictionary_id = nil
      end
      Field.new(fb_field.name, type, fb_field.nullable?, dictionary_id)
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

    def read_record_batch(fb_record_batch, schema, body)
      n_rows = fb_record_batch.length
      nodes = fb_record_batch.nodes
      buffers = fb_record_batch.buffers
      columns = schema.fields.collect do |field|
        read_column(field, nodes, buffers, body)
      end
      RecordBatch.new(schema, n_rows, columns)
    end

    def read_column(field, nodes, buffers, body)
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
        values = body.slice(values_buffer.offset, values_buffer.length)
        field.type.build_array(length, validity, values)
      when VariableSizeBinaryType
        offsets_buffer = buffers.shift
        values_buffer = buffers.shift
        offsets = body.slice(offsets_buffer.offset, offsets_buffer.length)
        values = body.slice(values_buffer.offset, values_buffer.length)
        field.type.build_array(length, validity, offsets, values)
      when FixedSizeBinaryType
        values_buffer = buffers.shift
        values = body.slice(values_buffer.offset, values_buffer.length)
        field.type.build_array(length, validity, values)
      when VariableSizeListType
        offsets_buffer = buffers.shift
        offsets = body.slice(offsets_buffer.offset, offsets_buffer.length)
        child = read_column(field.type.child, nodes, buffers, body)
        field.type.build_array(length, validity, offsets, child)
      when StructType
        children = field.type.children.collect do |child|
          read_column(child, nodes, buffers, body)
        end
        field.type.build_array(length, validity, children)
      when DenseUnionType
        # dense union type doesn't have validity.
        types = validity
        offsets_buffer = buffers.shift
        offsets = body.slice(offsets_buffer.offset, offsets_buffer.length)
        children = field.type.children.collect do |child|
          read_column(child, nodes, buffers, body)
        end
        field.type.build_array(length, types, offsets, children)
      when SparseUnionType
        # sparse union type doesn't have validity.
        types = validity
        children = field.type.children.collect do |child|
          read_column(child, nodes, buffers, body)
        end
        field.type.build_array(length, types, children)
      when DictionaryType
        indices_buffer = buffers.shift
        indices = body.slice(indices_buffer.offset, indices_buffer.length)
        dictionary = find_dictionary(field.dictionary_id)
        field.type.build_array(length, validity, indices, dictionary)
      end
    end
  end
end
