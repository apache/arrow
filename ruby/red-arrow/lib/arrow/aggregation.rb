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
  class Aggregation
    class << self
      # @api private
      def try_convert(value)
        case value
        when Hash
          function = value[:function]
          return nil if function.nil?
          function = function.to_s if function.is_a?(Symbol)
          return nil unless function.is_a?(String)
          # TODO: Improve this when we have non hash based aggregate function
          function = "hash_#{function}" unless function.start_with?("hash_")
          options = value[:options]
          input = value[:input]
          return nil if input.nil?
          output = value[:output]
          if output.nil?
            normalized_function = function.gsub(/\Ahash_/, "")
            output = "#{normalized_function}(#{input})"
          end
          new(function, options, input, output)
        else
          nil
        end
      end
    end
  end
end
