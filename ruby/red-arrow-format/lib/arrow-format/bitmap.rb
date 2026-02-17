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

    def initialize(buffer, offset, n_values)
      @buffer = buffer
      @offset = offset
      @n_values = n_values
    end

    def [](i)
      i += @offset
      (@buffer.get_value(:U8, i / 8) & (1 << (i % 8))) > 0
    end

    def each
      return to_enum(__method__) unless block_given?

      # TODO: Optimize
      current = -1
      n_bytes = (@offset + @n_values) / 8
      @buffer.each(:U8, 0, n_bytes) do |offset, value|
        7.times do |i|
          current += 1
          next if current < @offset
          yield((value & (1 << (i % 8))) > 0)
        end
      end
      remained_bits = (@offset + @n_values) % 8
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
