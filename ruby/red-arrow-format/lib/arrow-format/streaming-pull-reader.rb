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
require_relative "readable"
require_relative "record-batch"
require_relative "schema"
require_relative "type"

module ArrowFormat
  class MessagePullReader
    CONTINUATION_TYPE = :s32
    CONTINUATION_SIZE = IO::Buffer.size_of(CONTINUATION_TYPE)
    CONTINUATION_STRING = "\xFF\xFF\xFF\xFF".b.freeze
    CONTINUATION_INT32 = -1
    METADATA_LENGTH_TYPE = :s32
    METADATA_LENGTH_SIZE = IO::Buffer.size_of(METADATA_LENGTH_TYPE)

    def initialize(&on_read)
      @on_read = on_read
      @buffer = IO::Buffer.new(0)
      @metadata_length = nil
      @body_length = nil
      @state = :initial
    end

    def next_required_size
      case @state
      when :initial
        CONTINUATION_SIZE
      when :metadata_length
        METADATA_LENGTH_SIZE
      when :metadata
        @metadata_length
      when :body
        @body_length
      when :eos
        0
      end
    end

    def eos?
      @state == :eos
    end

    def consume(chunk)
      return if eos?

      if @buffer.size.zero?
        target = chunk
      else
        @buffer.resize(@buffer.size + chunk.size)
        @buffer.copy(chunk)
        target = @buffer
      end

      loop do
        next_size = next_required_size
        break if next_size.zero?

        if target.size < next_size
          @buffer.resize(target.size) if @buffer.size < target.size
          @buffer.copy(target)
          @buffer.resize(target.size)
          return
        end

        case @state
        when :initial
          consume_initial(target)
        when :metadata_length
          consume_metadata_length(target)
        when :metadata
          consume_metadata(target)
        when :body
          consume_body(target)
        end
        break if target.size == next_size

        target = target.slice(next_size)
      end
    end

    private
    def consume_initial(target)
      continuation = target.get_value(CONTINUATION_TYPE, 0)
      unless continuation == CONTINUATION_INT32
        raise ReadError.new("Invalid continuation token: " +
                            continuation.inspect)
      end
      @state = :metadata_length
    end

    def consume_metadata_length(target)
      length = target.get_value(METADATA_LENGTH_TYPE, 0)
      if length < 0
        raise ReadError.new("Negative metadata length: " +
                            length.inspect)
      end
      if length == 0
        @state = :eos
      else
        @metadata_length = length
        @state = :metadata
      end
    end

    def consume_metadata(target)
      metadata_buffer = target.slice(0, @metadata_length)
      @message = Org::Apache::Arrow::Flatbuf::Message.new(metadata_buffer)
      @body_length = @message.body_length
      if @body_length < 0
        raise ReadError.new("Negative body length: " +
                            @body_length.inspect)
      end
      @state = :body
      consume_body if @body_length.zero?
    end

    def consume_body(target=nil)
      body = target&.slice(0, @body_length)
      @on_read.call(@message, body)
      @state = :initial
    end
  end

  class StreamingPullReader
    include Readable

    attr_reader :schema
    def initialize(&on_read)
      @on_read = on_read
      @message_pull_reader = MessagePullReader.new do |message, body|
        process_message(message, body)
      end
      @state = :schema
      @schema = nil
    end

    def next_required_size
      @message_pull_reader.next_required_size
    end

    def eos?
      @message_pull_reader.eos?
    end

    def consume(chunk)
      @message_pull_reader.consume(chunk)
    end

    private
    def process_message(message, body)
      case @state
      when :schema
        process_schema_message(message, body)
      when :record_batch
        process_record_batch_message(message, body)
      end
    end

    def process_schema_message(message, body)
      header = message.header
      unless header.is_a?(Org::Apache::Arrow::Flatbuf::Schema)
        raise ReadError.new("Not a schema message: " +
                            header.inspect)
      end

      @schema = read_schema(header)
      # TODO: initial dictionaries support
      @state = :record_batch
    end

    def process_record_batch_message(message, body)
      header = message.header
      unless header.is_a?(Org::Apache::Arrow::Flatbuf::RecordBatch)
        raise ReadError.new("Not a record batch message: " +
                            header.inspect)
      end

      @on_read.call(read_record_batch(header, @schema, body))
    end
  end
end
