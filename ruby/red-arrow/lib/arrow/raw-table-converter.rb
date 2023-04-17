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
  class RawTableConverter
    attr_reader :n_rows
    attr_reader :schema
    attr_reader :values
    def initialize(raw_table)
      @raw_table = raw_table
      convert
    end

    private
    def convert
      if @raw_table.is_a?(::Array) and @raw_table[0].is_a?(Column)
        fields = @raw_table.collect(&:field)
        @schema = Schema.new(fields)
        @values = @raw_table.collect(&:data)
      else
        fields = []
        @values = []
        @raw_table.each do |name, array|
          if array.respond_to?(:to_arrow_chunked_array)
            chunked_array = array.to_arrow_chunked_array
          elsif array.respond_to?(:to_arrow_array)
            chunked_array = ChunkedArray.new([array.to_arrow_array])
          else
            array = array.to_ary if array.respond_to?(:to_ary)
            chunked_array = ChunkedArray.new([ArrayBuilder.build(array)])
          end
          fields << Field.new(name.to_s, chunked_array.value_data_type)
          @values << chunked_array
        end
        @schema = Schema.new(fields)
      end
      @n_rows = @values[0].length
    end
  end
end
