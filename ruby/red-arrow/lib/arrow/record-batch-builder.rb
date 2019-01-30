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
  class RecordBatchBuilder
    class << self
      # @since 0.12.0
      def build(schema, data)
        builder = new(schema)
        builder.append(data)
        builder.flush
      end
    end

    alias_method :initialize_raw, :initialize
    private :initialize_raw
    def initialize(schema)
      unless schema.is_a?(Schema)
        schema = Schema.new(schema)
      end
      initialize_raw(schema)
      @name_to_index = {}
      schema.fields.each_with_index do |field, i|
        @name_to_index[field.name] = i
      end
    end

    # @since 0.12.0
    def [](name_or_index)
      case name_or_index
      when String, Symbol
        name = name_or_index
        self[resolve_name(name)]
      else
        index = name_or_index
        column_builders[index]
      end
    end

    # @since 0.12.0
    def append(*values)
      values.each do |value|
        case value
        when Hash
          append_columns(value)
        else
          append_records(value)
        end
      end
    end

    # @since 0.12.0
    def append_records(records)
      n = n_fields
      columns = n.times.collect do
        []
      end
      records.each_with_index do |record, nth_record|
        case record
        when nil
        when Hash
          record.each do |name, value|
            nth_column = resolve_name(name)
            next if nth_column.nil?
            columns[nth_column] << value
          end
        else
          record.each_with_index do |value, nth_column|
            columns[nth_column] << value
          end
        end
        columns.each do |column|
          column << nil if column.size != (nth_record + 1)
        end
      end
      columns.each_with_index do |column, i|
        self[i].append(*column)
      end
    end

    # @since 0.12.0
    def append_columns(columns)
      columns.each do |name, values|
        self[name].append(*values)
      end
    end

    private
    def resolve_name(name)
      @name_to_index[name.to_s]
    end

    # TODO: Make public with good name. Is column_builders good enough?
    # builders? sub_builders?
    def column_builders
      @column_builders ||= n_fields.times.collect do |i|
        get_field(i)
      end
    end
  end
end
