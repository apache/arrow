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

    attr_reader :schema
    def initialize(input)
      @input = input
      @schema = nil
    end

    def each
      return to_enum(__method__) unless block_given?

      reader = StreamingPullReader.new do |record_batch|
        @schema ||= reader.schema
        yield(record_batch)
      end

      buffer = "".b
      loop do
        next_size = reader.next_required_size
        break if next_size.zero?

        next_chunk = @input.read(next_size, buffer)
        break if next_chunk.nil?

        reader.consume(IO::Buffer.for(next_chunk))
      end
    end
  end
end
