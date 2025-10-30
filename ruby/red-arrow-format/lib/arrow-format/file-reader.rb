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

require_relative "org/apache/arrow/flatbuf/footer"
require_relative "org/apache/arrow/flatbuf/message"
require_relative "org/apache/arrow/flatbuf/schema"

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
          buffers = header.buffers
          schema.fields.each do |field|
            columns << read_column(field, n_rows, buffers, body)
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

    def read_schema(fb_schema)
      fields = fb_schema.fields.collect do |fb_field|
        fb_type = fb_field.type
        case fb_type
        when Org::Apache::Arrow::Flatbuf::Int
          case fb_type.bit_width
          when 8
            if fb_type.signed?
              type = Int8Type.new
            else
              type = UInt8Type.new
            end
          end
        end
        Field.new(fb_field.name, type)
      end
      Schema.new(fields)
    end

    def read_column(field, n_rows, buffers, body)
      case field.type
      when UInt8Type
        validity_buffer = buffers.shift
        if validity_buffer.length.zero?
          validity = nil
        else
          validity = body.slice(validity_buffer.offset, validity_buffer.length)
        end

        values_buffer = buffers.shift
        values = body.slice(values_buffer.offset, values_buffer.length)
        UInt8Array.new(n_rows, validity, values)
      end
    end
  end
end
