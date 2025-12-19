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
require_relative "error"
require_relative "field"
require_relative "record-batch"
require_relative "schema"
require_relative "type"

require_relative "org/apache/arrow/flatbuf/binary"
require_relative "org/apache/arrow/flatbuf/bool"
require_relative "org/apache/arrow/flatbuf/date"
require_relative "org/apache/arrow/flatbuf/date_unit"
require_relative "org/apache/arrow/flatbuf/duration"
require_relative "org/apache/arrow/flatbuf/fixed_size_binary"
require_relative "org/apache/arrow/flatbuf/floating_point"
require_relative "org/apache/arrow/flatbuf/footer"
require_relative "org/apache/arrow/flatbuf/int"
require_relative "org/apache/arrow/flatbuf/large_binary"
require_relative "org/apache/arrow/flatbuf/large_list"
require_relative "org/apache/arrow/flatbuf/large_utf8"
require_relative "org/apache/arrow/flatbuf/list"
require_relative "org/apache/arrow/flatbuf/map"
require_relative "org/apache/arrow/flatbuf/message"
require_relative "org/apache/arrow/flatbuf/null"
require_relative "org/apache/arrow/flatbuf/precision"
require_relative "org/apache/arrow/flatbuf/schema"
require_relative "org/apache/arrow/flatbuf/struct_"
require_relative "org/apache/arrow/flatbuf/time"
require_relative "org/apache/arrow/flatbuf/timestamp"
require_relative "org/apache/arrow/flatbuf/time_unit"
require_relative "org/apache/arrow/flatbuf/union"
require_relative "org/apache/arrow/flatbuf/union_mode"
require_relative "org/apache/arrow/flatbuf/utf8"

module ArrowFormat
  class FileReader
    include Enumerable

    MAGIC = "ARROW1".b
    MAGIC_BUFFER = IO::Buffer.for(MAGIC)
    START_MARKER_SIZE = MAGIC_BUFFER.size
    END_MARKER_SIZE = MAGIC_BUFFER.size
    CONTINUATION = "\xFF\xFF\xFF\xFF".b
    CONTINUATION_BUFFER = IO::Buffer.for(CONTINUATION)
    # <magic number "ARROW1">
    # <empty padding bytes [to 8 byte boundary]>
    STREAMING_FORMAT_START_OFFSET = 8
    INT32_SIZE = 4
    FOOTER_SIZE_SIZE = INT32_SIZE
    METADATA_SIZE_SIZE = INT32_SIZE

    def initialize(input)
      case input
      when IO
        @buffer = IO::Buffer.map(input, nil, 0, IO::Buffer::READONLY)
      when String
        @buffer = IO::Buffer.for(input)
      else
        @buffer = input
      end

      validate
      @footer = read_footer
    end

    def each
      offset = STREAMING_FORMAT_START_OFFSET
      schema = nil
      continuation_size = CONTINUATION_BUFFER.size
      # streaming format
      loop do
        continuation = @buffer.slice(offset, continuation_size)
        unless continuation == CONTINUATION_BUFFER
          raise ReadError.new(@buffer, "No valid continuation")
        end
        offset += continuation_size

        metadata_size = @buffer.get_value(:u32, offset)
        offset += METADATA_SIZE_SIZE
        break if metadata_size.zero?

        metadata_data = @buffer.slice(offset, metadata_size)
        offset += metadata_size
        metadata = Org::Apache::Arrow::Flatbuf::Message.new(metadata_data)

        body = @buffer.slice(offset, metadata.body_length)
        header = metadata.header
        case header
        when Org::Apache::Arrow::Flatbuf::Schema
          schema = read_schema(header)
        when Org::Apache::Arrow::Flatbuf::RecordBatch
          n_rows = header.length
          columns = []
          nodes = header.nodes
          buffers = header.buffers
          schema.fields.each do |field|
            columns << read_column(field, nodes, buffers, body)
          end
          yield(RecordBatch.new(schema, n_rows, columns))
        end

        offset += metadata.body_length
      end
    end

    private
    def validate
      minimum_size = STREAMING_FORMAT_START_OFFSET +
                     FOOTER_SIZE_SIZE +
                     END_MARKER_SIZE
      if @buffer.size < minimum_size
        raise ReadError.new(@buffer,
                            "Input must be larger than or equal to " +
                            "#{minimum_size}: #{@buffer.size}")
      end

      start_marker = @buffer.slice(0, START_MARKER_SIZE)
      if start_marker != MAGIC_BUFFER
        raise ReadError.new(@buffer, "No start marker")
      end
      end_marker = @buffer.slice(@buffer.size - END_MARKER_SIZE, END_MARKER_SIZE)
      if end_marker != MAGIC_BUFFER
        raise ReadError.new(@buffer, "No end marker")
      end
    end

    def read_footer
      footer_size_offset = @buffer.size - END_MARKER_SIZE - FOOTER_SIZE_SIZE
      footer_size = @buffer.get_value(:u32, footer_size_offset)
      footer_data = @buffer.slice(footer_size_offset - footer_size, footer_size)
      Org::Apache::Arrow::Flatbuf::Footer.new(footer_data)
    end

    def read_field(fb_field)
      fb_type = fb_field.type
      case fb_type
      when Org::Apache::Arrow::Flatbuf::Null
        type = NullType.singleton
      when Org::Apache::Arrow::Flatbuf::Bool
        type = BooleanType.singleton
      when Org::Apache::Arrow::Flatbuf::Int
        case fb_type.bit_width
        when 8
          if fb_type.signed?
            type = Int8Type.singleton
          else
            type = UInt8Type.singleton
          end
        when 16
          if fb_type.signed?
            type = Int16Type.singleton
          else
            type = UInt16Type.singleton
          end
        when 32
          if fb_type.signed?
            type = Int32Type.singleton
          else
            type = UInt32Type.singleton
          end
        when 64
          if fb_type.signed?
            type = Int64Type.singleton
          else
            type = UInt64Type.singleton
          end
        end
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
      end
      Field.new(fb_field.name, type, fb_field.nullable?)
    end

    def read_schema(fb_schema)
      fields = fb_schema.fields.collect do |fb_field|
        read_field(fb_field)
      end
      Schema.new(fields)
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
      end
    end
  end
end
