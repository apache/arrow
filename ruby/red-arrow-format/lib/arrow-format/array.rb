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
  class Array
    attr_reader :type
    attr_reader :size
    alias_method :length, :size
    def initialize(type, size, validity_buffer, values_buffer)
      @type = type
      @size = size
      @validity_buffer = validity_buffer
      @values_buffer = values_buffer
    end

    def valid?(i)
      return true if @validity_buffer.nil?
      (@validity_buffer.get_value(:U8, i / 8) & (1 << (i % 8))) > 0
    end

    def null?(i)
      not valid?(i)
    end

    private
    def apply_validity(array)
      return array if @validity_buffer.nil?
      n_bytes = @size / 8
      @validity_buffer.each(:U8, 0, n_bytes) do |offset, value|
        7.times do |i|
          array[offset * 8 + i] = nil if (value & (1 << (i % 8))).zero?
        end
      end
      remained_bits = @size % 8
      unless remained_bits.zero?
        value = @validity_buffer.get_value(:U8, n_bytes)
        remained_bits.times do |i|
          array[n_bytes * 8 + i] = nil if (value & (1 << (i % 8))).zero?
        end
      end
      array
    end
  end

  class Int8Array < Array
    def to_a
      apply_validity(@values_buffer.values(:S8, 0, @size))
    end
  end

  class UInt8Array < Array
    def to_a
      apply_validity(@values_buffer.values(:U8, 0, @size))
    end
  end
end
