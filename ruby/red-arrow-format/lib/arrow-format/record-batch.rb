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
require_relative "record"

module ArrowFormat
  class RecordBatch
    include BufferAlignable

    attr_reader :schema
    attr_reader :n_rows
    alias_method :size, :n_rows
    alias_method :length, :n_rows
    attr_reader :columns
    attr_reader :message_metadata
    def initialize(*args, message_metadata: nil)
      n_args = args.size
      args = build(args[0]) if n_args == 1
      if args.size != 3
        message = "wrong number of arguments (given #{n_args}, expected 1 or 3)"
        raise ArgumentError, message
      end
      schema, n_rows, columns = args
      @schema = schema
      @n_rows = n_rows
      @columns = columns
      @message_metadata = message_metadata
    end

    def empty?
      @n_rows.zero?
    end

    def find_column(name_or_index)
      case name_or_index
      when Integer
        @columns[name_or_index]
      when Symbol
        name_to_column[name_or_index.to_s]
      else
        name_to_column[name_or_index.to_str]
      end
    end

    def each_record(reuse_record: false)
      unless block_given?
        return to_enum(__method__, reuse_record: reuse_record)
      end

      if reuse_record
        record = Record.new(self, nil)
        @n_rows.times do |i|
          record.index = i
          yield(record)
        end
      else
        @n_rows.times do |i|
          yield(Record.new(self, i))
        end
      end
    end

    def records
      size.times.collect do |i|
        Record.new(self, i)
      end
    end

    def to_h
      name_to_column.dup
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

    private
    def build(data)
      records = nil
      fields = []
      columns = []
      mode = nil
      i = 0
      data.each do |name, column|
        if i.zero?
          if column.nil?
            mode = :record
            records = {}
          else
            mode = :column
          end
        end

        if mode == :record
          record = name
          record.each do |n, value|
            values = (records[n] ||= [])
            while values.size < i
              values << nil
            end
            values << value
          end
        else
          fields << Field.new(name, column.type)
          columns << column
        end
        i += 1
      end

      if mode == :record
        records.each do |name, values|
          column = Array.build(values)
          fields << Field.new(name, column.type)
          columns << column
        end
      end

      raise ArgumentError, "no data" if columns.empty?
      all_n_rows = columns.collect(&:size)
      if all_n_rows.uniq.size != 1
        message =
          "inconsistent the number of rows: #{all_n_rows.join(", ")}"
        raise ArgumentError, message
      end

      return Schema.new(fields), all_n_rows[0], columns
    end

    def name_to_column
      @name_to_column ||= build_name_to_column
    end

    def build_name_to_column
      name_to_column = {}
      @schema.fields.zip(@columns) do |field, column|
        name_to_column[field.name] = column
      end
      name_to_column.freeze
      name_to_column
    end
  end
end
