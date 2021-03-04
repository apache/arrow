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
  class Column
    include Enumerable

    attr_reader :container
    attr_reader :field
    attr_reader :data
    def initialize(container, index)
      @container = container
      @index = index
      @field = @container.schema[@index]
      @data = @container.get_column_data(@index)
    end

    def name
      @field.name
    end

    def data_type
      @field.data_type
    end

    def null?(i)
      @data.null?(i)
    end

    def valid?(i)
      @data.valid?(i)
    end

    def [](i)
      @data[i]
    end

    def each(&block)
      @data.each(&block)
    end

    def reverse_each(&block)
      @data.reverse_each(&block)
    end

    def n_rows
      @data.n_rows
    end
    alias_method :size, :n_rows
    alias_method :length, :n_rows

    def n_nulls
      @data.n_nulls
    end

    def ==(other)
      other.is_a?(self.class) and
        @field == other.field and
        @data == other.data
    end
  end
end
