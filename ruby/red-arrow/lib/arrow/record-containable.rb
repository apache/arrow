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
  module RecordContainable
    def each_column(&block)
      return to_enum(__method__) unless block_given?

      columns.each(&block)
    end

    def each_record(reuse_record: false)
      unless block_given?
        return to_enum(__method__, reuse_record: reuse_record)
      end

      if reuse_record
        record = Record.new(self, nil)
        n_rows.times do |i|
          record.index = i
          yield(record)
        end
      else
        n_rows.times do |i|
          yield(Record.new(self, i))
        end
      end
    end

    def find_column(name_or_index)
      case name_or_index
      when String, Symbol
        name = name_or_index.to_s
        index = resolve_column_name(name)
        return nil if index.nil?
        columns[index]
      when Integer
        index = name_or_index
        columns[index]
      else
        message = "column name or index must be String, Symbol or Integer"
        raise ArgumentError, message
      end
    end

    private
    def resolve_column_name(name)
      (@column_name_to_index ||= build_column_name_resolve_table)[name]
    end

    def build_column_name_resolve_table
      table = {}
      schema.fields.each_with_index do |field, i|
        table[field.name] = i
      end
      table
    end
  end
end
