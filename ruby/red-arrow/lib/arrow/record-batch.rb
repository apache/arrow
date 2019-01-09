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

require "arrow/record-containable"

module Arrow
  class RecordBatch
    include RecordContainable
    include Enumerable

    alias_method :each, :each_record

    alias_method :columns_raw, :columns
    def columns
      @columns ||= columns_raw
    end

    # Converts the record batch to {Arrow::Table}.
    #
    # @return [Arrow::Table]
    #
    # @since 0.12.0
    def to_table
      Table.new(schema, [self])
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
