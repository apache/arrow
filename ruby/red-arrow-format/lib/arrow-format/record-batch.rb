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
  class RecordBatch
    include BufferAlignable

    attr_reader :schema
    attr_reader :n_rows
    attr_reader :columns
    def initialize(schema, n_rows, columns)
      @schema = schema
      @n_rows = n_rows
      @columns = columns
    end

    def to_h
      hash = {}
      @schema.fields.zip(@columns) do |field, column|
        hash[field.name] = column
      end
      hash
    end

    def to_flatbuffers
      fb_record_batch = FB::RecordBatch::Data.new
      fb_record_batch.length = @n_rows
      fb_record_batch.nodes = all_columns_enumerator.collect do |array|
        field_node = FB::FieldNode::Data.new
        field_node.length = array.size
        field_node.null_count = array.n_nulls
        field_node
      end
      offset = 0
      fb_record_batch.buffers = all_buffers_enumerator.collect do |buffer|
        fb_buffer = FB::Buffer::Data.new
        fb_buffer.offset = offset
        if buffer
          aligned_size = aligned_buffer_size(buffer)
          offset += aligned_size
          fb_buffer.length = aligned_size
        else
          fb_buffer.length = 0
        end
        fb_buffer
      end
      # body_compression = FB::BodyCompression::Data.new
      # body_compression.codec = ...
      # fb_record_batch.compression = body_compression
      fb_record_batch
    end

    # Pre-order depth-first traversal
    def all_columns_enumerator
      Enumerator.new do |yielder|
        traverse = lambda do |array|
          yielder << array
          if array.respond_to?(:child)
            traverse.call(array.child)
          elsif array.respond_to?(:children)
            array.children.each do |child_array|
              traverse.call(child_array)
            end
          end
        end
        @columns.each do |array|
          traverse.call(array)
        end
      end
    end

    def all_buffers_enumerator
      Enumerator.new do |yielder|
        all_columns_enumerator.each do |array|
          array.each_buffer do |buffer|
            yielder << buffer
          end
        end
      end
    end
  end
end
