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

require_relative "flat-buffers"

module ArrowFormat
  class StreamingWriter
    include FlatBuffers::Alignable

    ALIGNMENT_SIZE = IO::Buffer.size_of(:u64)
    CONTINUATION = "\xFF\xFF\xFF\xFF".b.freeze
    EOS = "\xFF\xFF\xFF\xFF\x00\x00\x00\x00".b.freeze
    METADATA_LARGEST_PADDING = "\x00" * 7

    def initialize(output)
      @output = output
      @offset = 0
      @fb_record_batch_blocks = []
    end

    def start(schema)
      write_message(build_metadata(schema.to_flat_buffers))
      # TODO: Write dictionaries
    end

    def write_record_batch(record_batch)
      body_length = 0
      record_batch.all_buffers_enumerator.each do |buffer|
        body_length += buffer.size if buffer
      end
      metadata = build_metadata(record_batch.to_flat_buffers, body_length)
      fb_block = FB::Block::Data.new
      fb_block.offset = @offset
      fb_block.meta_data_length =
        CONTINUATION.bytesize +
        MessagePullReader::METADATA_LENGTH_SIZE +
        metadata.bytesize
      fb_block.body_length = body_length
      @fb_record_batch_blocks << fb_block
      write_message(metadata) do
        record_batch.all_buffers_enumerator.each do |buffer|
          write_data(buffer) if buffer
        end
      end
    end

    # TODO
    # def write_dictionary_delta(id, dictionary)
    # end

    def finish
      write_data(EOS)
      @output
    end

    private
    def write_data(data)
      @output << data
      @offset += data.bytesize
    end

    def build_metadata(header, body_length=0)
      fb_message = FB::Message::Data.new
      fb_message.version = FB::MetadataVersion::V5
      fb_message.header = header
      fb_message.body_length = body_length
      metadata = FB::Message.serialize(fb_message)
      metadata_size = metadata.bytesize
      padding_size = compute_padding_size(metadata_size, ALIGNMENT_SIZE)
      metadata_size += padding_size
      align!(metadata, ALIGNMENT_SIZE)
      metadata
    end

    def write_message(metadata)
      write_data(CONTINUATION)
      metadata_size = metadata.bytesize
      write_data([metadata_size].pack("l<"))
      write_data(metadata)
      yield if block_given?
    end
  end
end
