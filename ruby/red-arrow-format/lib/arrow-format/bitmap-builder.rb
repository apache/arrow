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
  using FlatBuffers::AppendAsBytes if FlatBuffers.const_defined?(:AppendAsBytes)

  class DenseBitmapBuilder
    include BufferAlignable

    def initialize
      @buffer = +"".b
      @n_bits = 0
      @byte = 0
    end

    def append(value)
      @byte |= 1 << @n_bits if value
      @n_bits += 1
      flush if @n_bits == 8
      self
    end

    def finish
      flush if @n_bits > 0
      pad!(@buffer, buffer_padding_size(@buffer))
      @buffer.freeze
      IO::Buffer.for(@buffer)
    end

    private
    def flush
      @buffer.append_as_bytes([@byte].pack("C"))
      @n_bits = 0
      @byte = 0
    end
  end

  class SparseBitmapBuilder
    include BufferAlignable

    def initialize
      @unset_indexes = {}
    end

    def unset(index)
      @unset_indexes[index] = true
    end

    def finish(size)
      builder = DenseBitmapBuilder.new
      set_value = true
      unset_value = false
      if @unset_indexes.empty?
        size.times do
          builder.append(set_value)
        end
      else
        previous_index = 0
        @unset_indexes.keys.sort.each do |index|
          previous_index.upto(index - 1) do
            builder.append(set_value)
          end
          builder.append(unset_value)
          previous_index = index + 1
        end
        (size - previous_index).times do
          builder.append(set_value)
        end
      end
      builder.finish
    end
  end
end
