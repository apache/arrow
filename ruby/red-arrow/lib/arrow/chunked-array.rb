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

module Arrow
  class ChunkedArray
    include Enumerable

    include ArrayComputable
    include GenericFilterable
    include GenericTakeable
    include InputReferable

    def to_arrow
      self
    end

    def to_arrow_array
      combine
    end

    def to_arrow_chunked_array
      self
    end

    alias_method :size, :n_rows
    unless method_defined?(:length)
      alias_method :length, :n_rows
    end

    alias_method :chunks_raw, :chunks
    def chunks
      @chunks ||= chunks_raw.tap do |_chunks|
        _chunks.each do |chunk|
          share_input(chunk)
        end
      end
    end

    alias_method :get_chunk_raw, :get_chunk
    def get_chunk(i)
      chunks[i]
    end

    def null?(i)
      chunks.each do |array|
        return array.null?(i) if i < array.length
        i -= array.length
      end
      nil
    end

    def valid?(i)
      chunks.each do |array|
        return array.valid?(i) if i < array.length
        i -= array.length
      end
      nil
    end

    def [](i)
      i += length if i < 0
      chunks.each do |array|
        return array[i] if i < array.length
        i -= array.length
      end
      nil
    end

    def each(&block)
      return to_enum(__method__) unless block_given?

      chunks.each do |array|
        array.each(&block)
      end
    end

    def reverse_each(&block)
      return to_enum(__method__) unless block_given?

      chunks.reverse_each do |array|
        array.reverse_each(&block)
      end
    end

    def each_chunk(&block)
      chunks.each(&block)
    end

    def pack
      first_chunk = chunks.first
      data_type = first_chunk.value_data_type
      case data_type
      when TimestampDataType
        builder = TimestampArrayBuilder.new(data_type)
        builder.build(to_a)
      else
        first_chunk.class.new(to_a)
      end
    end

    def count(options: nil)
      compute("count", options: options).value
    end

    def sum(options: nil)
      compute("sum", options: options).value
    end

    def unique
      compute("unique")
    end

    def cast(target_data_type, options: nil)
      casted_chunks = chunks.collect do |chunk|
        chunk.cast(target_data_type, options)
      end
      self.class.new(casted_chunks)
    end
  end
end
