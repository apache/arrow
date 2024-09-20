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

class TestStreamDecoder < Test::Unit::TestCase
  include Helper::Buildable

  class Listener < Arrow::StreamListener
    type_register

    attr_reader :events
    def initialize
      super
      @events = []
    end

    private
    def virtual_do_on_eos
      @events << [:eos]
      true
    end

    def virtual_do_on_record_batch_decoded(record_batch, metadata)
      @events << [:record_batch_decoded, record_batch, metadata]
      true
    end

    def virtual_do_on_schema_decoded(schema, filtered_schema)
      @events << [:schema_decoded, schema, filtered_schema]
      true
    end
  end

  def setup
    columns = {
      "enabled": build_boolean_array([true, false, nil, true]),
    }
    @record_batch = build_record_batch(columns)
    @schema = @record_batch.schema

    @buffer = Arrow::ResizableBuffer.new(0)
    output = Arrow::BufferOutputStream.new(@buffer)
    stream_writer = Arrow::RecordBatchStreamWriter.new(output, @schema)
    stream_writer.write_record_batch(@record_batch)
    stream_writer.close
    output.close

    @listener = Listener.new
    @decoder = Arrow::StreamDecoder.new(@listener)
  end

  def test_listener
    assert_equal(@listener, @decoder.listener)
  end

  def test_consume_bytes
    @buffer.data.to_s.each_byte do |byte|
      @decoder.consume_bytes(GLib::Bytes.new(byte.chr))
    end
    assert_equal([
                   [:schema_decoded, @schema, @schema],
                   [:record_batch_decoded, @record_batch, nil],
                   [:eos],
                 ],
                 @listener.events)
  end

  def test_consume_buffer
    @buffer.data.to_s.each_byte do |byte|
      @decoder.consume_buffer(Arrow::Buffer.new(byte.chr))
    end
    assert_equal([
                   [:schema_decoded, @schema, @schema],
                   [:record_batch_decoded, @record_batch, nil],
                   [:eos],
                 ],
                 @listener.events)
  end

  def test_reset
    @decoder.consume_bytes(@buffer.data.to_s[0, 10])
    @decoder.reset
    @decoder.consume_bytes(@buffer.data)
    assert_equal([
                   [:schema_decoded, @schema, @schema],
                   [:record_batch_decoded, @record_batch, nil],
                   [:eos],
                 ],
                 @listener.events)
  end

  def test_schema
    assert_nil(@decoder.schema)
    @decoder.consume_bytes(@buffer.data)
    assert_equal(@schema, @decoder.schema)
  end

  def test_next_required_size
    data = @buffer.data.to_s
    loop do
      next_required_size = @decoder.next_required_size
      break if next_required_size.zero?
      @decoder.consume_bytes(data[0, next_required_size])
      data = data[next_required_size..-1]
    end
    assert_equal([
                   [:schema_decoded, @schema, @schema],
                   [:record_batch_decoded, @record_batch, nil],
                   [:eos],
                 ],
                 @listener.events)
  end
end
