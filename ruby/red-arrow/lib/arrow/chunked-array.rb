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

    alias_method :chunks_raw, :chunks
    def chunks
      @chunks ||= chunks_raw
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
  end
end
