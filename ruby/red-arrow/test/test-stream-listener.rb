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

class TestStreamListener < Test::Unit::TestCase
  class Listener < Arrow::StreamListener
    attr_reader :events
    def initialize
      super
      @events = []
    end

    def on_eos
      @events << [:eos]
    end

    def on_record_batch_decoded(record_batch, metadata)
      @events << [:record_batch_decoded, record_batch, metadata]
    end

    def on_schema_decoded(schema, filtered_schema)
      @events << [:schema_decoded, schema, filtered_schema]
    end
  end

  def setup
    @record_batch = Arrow::RecordBatch.new(enabled: [true, false, nil, true])
    @schema = @record_batch.schema

    @buffer = Arrow::ResizableBuffer.new(0)
    table = Arrow::Table.new(@schema, [@record_batch])
    table.save(@buffer, format: :stream)

    @listener = Listener.new
    @decoder = Arrow::StreamDecoder.new(@listener)
  end

  def test_consume
    @decoder.consume(@buffer)
    assert_equal([
                   [:schema_decoded, @schema, @schema],
                   [:record_batch_decoded, @record_batch, nil],
                   [:eos],
                 ],
                 @listener.events)
  end
end
