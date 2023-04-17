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

require "arrow/raw-table-converter"

module Arrow
  class RecordBatch
    include Enumerable

    include ColumnContainable
    include InputReferable
    include RecordContainable

    class << self
      def new(*args)
        n_args = args.size
        case n_args
        when 1
          raw_table_converter = RawTableConverter.new(args[0])
          n_rows = raw_table_converter.n_rows
          schema = raw_table_converter.schema
          values = raw_table_converter.values
          super(schema, n_rows, values)
        when 2
          schema, data = args
          RecordBatchBuilder.build(schema, data)
        when 3
          super
        else
          message = "wrong number of arguments (given #{n_args}, expected 1..3)"
          raise ArgumentError, message
        end
      end
    end

    alias_method :each, :each_record

    alias_method :size, :n_rows
    alias_method :length, :n_rows

    # Converts the record batch to {Arrow::Table}.
    #
    # @return [Arrow::Table]
    #
    # @since 0.12.0
    def to_table
      table = Table.new(schema, [self])
      share_input(table)
      table
    end

    def respond_to_missing?(name, include_private)
      return true if find_column(name)
      super
    end

    def method_missing(name, *args, &block)
      if args.empty?
        column = find_column(name)
        return column if column
      end
      super
    end
  end
end
