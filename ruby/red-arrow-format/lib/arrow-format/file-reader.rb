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

require_relative "streaming-reader"

module ArrowFormat
  class FileReader
    include Enumerable
    include Readable

    MAGIC = "ARROW1".b.freeze
    MAGIC_BUFFER = IO::Buffer.for(MAGIC)
    START_MARKER_SIZE = MAGIC_BUFFER.size
    END_MARKER_SIZE = MAGIC_BUFFER.size
    # <magic number "ARROW1">
    # <empty padding bytes [to 8 byte boundary]>
    STREAMING_FORMAT_START_OFFSET = 8
    CONTINUATION_BUFFER =
      IO::Buffer.for(MessagePullReader::CONTINUATION_STRING)
    FOOTER_SIZE_FORMAT = :s32
    FOOTER_SIZE_SIZE = IO::Buffer.size_of(FOOTER_SIZE_FORMAT)

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
      @record_batch_blocks = @footer.record_batches
      @schema = read_schema(@footer.schema)
      @dictionaries = read_dictionaries
    end

    def n_record_batches
      @record_batch_blocks.size
    end

    def read(i)
      fb_message, body = read_block(@record_batch_blocks[i], :record_batch, i)
      fb_header = fb_message.header
      unless fb_header.is_a?(FB::RecordBatch)
        raise FileReadError.new(@buffer,
                                "Not a record batch message: #{i}: " +
                                fb_header.class.name)
      end
      read_record_batch(fb_header, @schema, body)
    end

    def each
      return to_enum(__method__) {n_record_batches} unless block_given?

      @record_batch_blocks.size.times do |i|
        yield(read(i))
      end
    end

    private
    def validate
      minimum_size = STREAMING_FORMAT_START_OFFSET +
                     FOOTER_SIZE_SIZE +
                     END_MARKER_SIZE
      if @buffer.size < minimum_size
        raise FileReadError.new(@buffer,
                                "Input must be larger than or equal to " +
                                "#{minimum_size}: #{@buffer.size}")
      end

      start_marker = @buffer.slice(0, START_MARKER_SIZE)
      if start_marker != MAGIC_BUFFER
        raise FileReadError.new(@buffer, "No start marker")
      end
      end_marker = @buffer.slice(@buffer.size - END_MARKER_SIZE,
                                 END_MARKER_SIZE)
      if end_marker != MAGIC_BUFFER
        raise FileReadError.new(@buffer, "No end marker")
      end
    end

    def read_footer
      footer_size_offset = @buffer.size - END_MARKER_SIZE - FOOTER_SIZE_SIZE
      footer_size = @buffer.get_value(FOOTER_SIZE_FORMAT, footer_size_offset)
      footer_data = @buffer.slice(footer_size_offset - footer_size,
                                  footer_size)
      FB::Footer.new(footer_data)
    end

    def read_block(block, type, i)
      offset = block.offset

      # If we can report property error information, we can use
      # MessagePullReader here.
      #
      # message_pull_reader = MessagePullReader.new do |message, body|
      #   return read_record_batch(message.header, @schema, body)
      # end
      # chunk = @buffer.slice(offset,
      #                       MessagePullReader::CONTINUATION_SIZE +
      #                       MessagePullReader::METADATA_LENGTH_SIZE +
      #                       block.meta_data_length +
      #                       block.body_length)
      # message_pull_reader.consume(chunk)

      continuation_size = CONTINUATION_BUFFER.size
      continuation = @buffer.slice(offset, continuation_size)
      unless continuation == CONTINUATION_BUFFER
        raise FileReadError.new(@buffer,
                                "Invalid continuation: #{type}: #{i}: " +
                                continuation.inspect)
      end
      offset += continuation_size

      metadata_length_type = MessagePullReader::METADATA_LENGTH_TYPE
      metadata_length_size = MessagePullReader::METADATA_LENGTH_SIZE
      metadata_length = @buffer.get_value(metadata_length_type, offset)
      expected_metadata_length =
        block.meta_data_length -
        continuation_size -
        metadata_length_size
      unless metadata_length == expected_metadata_length
        raise FileReadError.new(@buffer,
                                "Invalid metadata length: #{type}: #{i}: " +
                                "expected:#{expected_metadata_length} " +
                                "actual:#{metadata_length}")
      end
      offset += metadata_length_size

      metadata = @buffer.slice(offset, metadata_length)
      fb_message = FB::Message.new(metadata)
      offset += metadata_length

      body = @buffer.slice(offset, block.body_length)

      [fb_message, body]
    end

    def read_dictionaries
      dictionary_blocks = @footer.dictionaries
      return nil if dictionary_blocks.nil?

      dictionary_fields = {}
      @schema.fields.each do |field|
        next unless field.type.is_a?(DictionaryType)
        dictionary_fields[field.dictionary_id] = field
      end

      dictionaries = {}
      dictionary_blocks.each_with_index do |block, i|
        fb_message, body = read_block(block, :dictionary_block, i)
        fb_header = fb_message.header
        unless fb_header.is_a?(FB::DictionaryBatch)
          raise FileReadError.new(@buffer,
                                  "Not a dictionary batch message: " +
                                  fb_header.inspect)
        end

        id = fb_header.id
        if fb_header.delta?
          unless dictionaries.key?(id)
            raise FileReadError.new(@buffer,
                                    "A delta dictionary batch message " +
                                    "must exist after a non delta " +
                                    "dictionary batch message: " +
                                    fb_header.inspect)
          end
        else
          if dictionaries.key?(id)
            raise FileReadError.new(@buffer,
                                    "Multiple non delta dictionary batch " +
                                    "messages for the same ID is invalid: " +
                                    fb_header.inspect)
          end
        end

        value_type = dictionary_fields[id].type.value_type
        schema = Schema.new([Field.new("dummy", value_type, true, nil)])
        record_batch = read_record_batch(fb_header.data, schema, body)
        if fb_header.delta?
          dictionaries[id] << record_batch.columns[0]
        else
          dictionaries[id] = [record_batch.columns[0]]
        end
      end
      dictionaries
    end

    def find_dictionaries(id)
      @dictionaries[id]
    end
  end
end
