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

module ArrowFlight
  class CallOptions
    class << self
      def try_convert(value)
        case value
        when Hash
          options = new
          value.each do |name, value|
            options.__send__("#{name}=", value)
          end
          options
        else
          nil
        end
      end
    end

    def headers=(headers)
      clear_headers
      headers.each do |name, value|
        add_header(name, value)
      end
    end

    def each_header
      return to_enum(__method__) unless block_given?
      foreach_header do |key, value|
        yield(key, value)
      end
    end

    def headers
      each_header.to_a
    end
  end
end
