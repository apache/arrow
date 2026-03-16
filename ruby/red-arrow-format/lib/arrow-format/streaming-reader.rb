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

require_relative "streaming-pull-reader"

module ArrowFormat
  class StreamingReader
    include Enumerable

    def initialize(input)
      @input = input
      @on_read = nil
      @pull_reader = StreamingPullReader.new do |record_batch|
        @on_read.call(record_batch) if @on_read
      end
      @buffer = "".b
      ensure_schema
    end

    def schema
      @pull_reader.schema
    end

    def each(&block)
      return to_enum(__method__) unless block_given?

      @on_read = block
      begin
        loop do
          break unless consume
        end
      ensure
        @on_read = nil
      end
    end

    private
    def consume
      next_size = @pull_reader.next_required_size
      return false if next_size.zero?

      next_chunk = @input.read(next_size, @buffer)
      return false if next_chunk.nil?

      @pull_reader.consume(IO::Buffer.for(next_chunk))
      true
    end

    def ensure_schema
      loop do
        break unless consume
        break if @pull_reader.schema
      end
    end
  end
end
