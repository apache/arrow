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

module ArrowFormat
  class Bitmap
    include Enumerable

    attr_reader :offset
    attr_reader :size
    alias_method :length, :size
    def initialize(buffer, offset, size)
      @buffer = buffer
      @offset = offset
      @size = size
    end

    def ==(other)
      return false unless other.is_a?(self.class)
      return false unless @size == other.size
      lazy.zip(other).all? do |bit, other_bit|
        bit == other_bit
      end
    end

    def [](i)
      return false if @buffer.nil?

      i += @offset
      (@buffer.get_value(:U8, i / 8) & (1 << (i % 8))) > 0
    end

    def each
      return to_enum(__method__) {@size} unless block_given?

      if @buffer.nil?
        @size.times do
          yield(false)
        end
        return
      end

      # TODO: Optimize
      current = -1
      n_bytes = (@offset + @size) / 8
      @buffer.each(:U8, 0, n_bytes) do |offset, value|
        8.times do |i|
          current += 1
          next if current < @offset
          yield((value & (1 << (i % 8))) > 0)
        end
      end
      remained_bits = (@offset + @size) % 8
      unless remained_bits.zero?
        value = @buffer.get_value(:U8, n_bytes)
        remained_bits.times do |i|
          current += 1
          next if current < @offset
          yield((value & (1 << (i % 8))) > 0)
        end
      end
    end

    def popcount
      # TODO: Optimize
      count do |flaged|
        flaged
      end
    end
  end
end
