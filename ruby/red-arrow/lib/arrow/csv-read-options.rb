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
  class CSVReadOptions
    alias_method :add_column_type_raw, :add_column_type
    def add_column_type(name, type)
      add_column_type_raw(name, DataType.resolve(type))
    end

    alias_method :delimiter_raw, :delimiter
    def delimiter
      delimiter_raw.chr
    end

    alias_method :delimiter_raw=, :delimiter=
    def delimiter=(delimiter)
      case delimiter
      when String
        if delimiter.bytesize != 1
          message = "delimiter must be 1 byte character: #{delimiter.inspect}"
          raise ArgumentError, message
        end
        delimiter = delimiter.ord
      end
      self.delimiter_raw = delimiter
    end
  end
end
