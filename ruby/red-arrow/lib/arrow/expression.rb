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
  class Expression
    class << self
      # @api private
      def try_convert(value)
        case value
        when Symbol
          FieldExpression.new(value.to_s)
        when ::Array
          function_name, *arguments = value
          case function_name
          when String, Symbol
            function_name = function_name.to_s
          else
            return nil
          end
          options = nil
          if arguments.last.is_a?(FunctionOptions)
            options = arguments.pop
          elsif arguments.last.is_a?(Hash)
            function = Function.find(function_name)
            if function
              options = function.resolve_options(arguments.pop)
            end
          end
          CallExpression.new(function_name, arguments, options)
        else
          datum = Datum.try_convert(value)
          return nil if datum.nil?
          LiteralExpression.new(datum)
        end
      end
    end
  end
end
