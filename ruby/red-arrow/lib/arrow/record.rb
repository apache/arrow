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
  class Record
    attr_reader :container
    attr_accessor :index
    def initialize(container, index)
      @container = container
      @index = index
    end

    def [](column_name_or_column_index)
      column = @container.find_column(column_name_or_column_index)
      return nil if column.nil?
      column[@index]
    end

    def to_a
      @container.columns.collect do |column|
        column[@index]
      end
    end

    def to_h
      attributes = {}
      @container.columns.each do |column|
        attributes[column.name] = column[@index]
      end
      attributes
    end

    def respond_to_missing?(name, include_private)
      return true if @container.find_column(name)
      super
    end

    def method_missing(name, *args, &block)
      if args.empty?
        column = @container.find_column(name)
        return column[@index] if column
      end
      super
    end
  end
end
