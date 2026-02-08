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

require_relative "buffer-alignable"

module ArrowFormat
  class StreamingWriter
    include BufferAlignable

    ALIGNMENT_SIZE = IO::Buffer.size_of(:u64)
    CONTINUATION = "\xFF\xFF\xFF\xFF".b.freeze
    EOS = "\xFF\xFF\xFF\xFF\x00\x00\x00\x00".b.freeze
    METADATA_LARGEST_PADDING = "\x00" * 7

    def initialize(output)
      @output = output
      @offset = 0
      @fb_dictionary_blocks = []
      @fb_record_batch_blocks = []
      @written_dictionary_offsets = {}
    end

    def start(schema)
      write_message(build_metadata(schema.to_flatbuffers))
    end

    def write_record_batch(record_batch)
      record_batch.schema.fields.each_with_index do |field, i|
        next if field.dictionary_id.nil?
        dictionary_array = record_batch.columns[i]
        write_dictionary(field.dictionary_id, dictionary_array)
      end

      write_record_batch_based_message(record_batch,
                                       record_batch.to_flatbuffers,
                                       @fb_record_batch_blocks)
    end

    def finish
      write_data(EOS)
      @output
    end

    private
    def write_data(data)
      case data
      when IO::Buffer
        # TODO: We should use IO::Buffer#write to avoid needless copy.
        # data.write(@output)
        @output << data.get_string
        @offset += data.size
      else
        @output << data
        @offset += data.bytesize
      end
    end

    def write_buffer(buffer)
      write_data(buffer)
      padding_size = buffer_padding_size(buffer)
      write_data(padding(padding_size)) if padding_size > 0
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

    def write_record_batch_based_message(record_batch, fb_header, fb_blocks)
      body_length = 0
      record_batch.all_buffers_enumerator.each do |buffer|
        body_length += aligned_buffer_size(buffer) if buffer
      end
      metadata = build_metadata(fb_header, body_length)
      fb_block = FB::Block::Data.new
      fb_block.offset = @offset
      fb_block.meta_data_length =
        CONTINUATION.bytesize +
        MessagePullReader::METADATA_LENGTH_SIZE +
        metadata.bytesize
      fb_block.body_length = body_length
      fb_blocks << fb_block
      write_message(metadata) do
        record_batch.all_buffers_enumerator.each do |buffer|
          write_buffer(buffer) if buffer
        end
      end
    end

    def write_dictionary(id, dictionary_array)
      value_type = dictionary_array.type.value_type
      dictionary = dictionary_array.dictionary

      offset = @written_dictionary_offsets[id]
      if offset.nil?
        is_delta = false
      else
        is_delta = true
        raise NotImplementedError,
              "Delta dictionary message isn't implemented yet"
      end

      schema = Schema.new([Field.new("dummy", value_type, true, nil)])
      size = dictionary.size
      record_batch = RecordBatch.new(schema, size, [dictionary])
      fb_dictionary_batch = FB::DictionaryBatch::Data.new
      fb_dictionary_batch.id = id
      fb_dictionary_batch.data = record_batch.to_flatbuffers
      fb_dictionary_batch.delta = is_delta
      write_record_batch_based_message(record_batch,
                                       fb_dictionary_batch,
                                       @fb_dictionary_blocks)
      @written_dictionary_offsets[id] = dictionary_array.dictionary.size
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
