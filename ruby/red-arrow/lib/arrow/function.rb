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
  class Function
    alias_method :execute_raw, :execute
    def execute(args, options=nil, context=nil)
      options = resolve_options(options)
      execute_raw(args, options, context)
    end
    alias_method :call, :execute

    def resolve_options(options)
      return nil if options.nil?
      return options if options.is_a?(FunctionOptions)

      arrow_options_class = options_type&.to_class
      if arrow_options_class
        if arrow_options_class.respond_to?(:try_convert)
          arrow_options = arrow_options_class.try_convert(options)
          return arrow_options if arrow_options
        end
        arrow_options = (default_options || arrow_options_class.new)
      else
        arrow_options = default_options
      end
      return arrow_options if arrow_options.nil?

      options.each do |key, value|
        setter = :"#{key}="
        next unless arrow_options.respond_to?(setter)
        arrow_options.__send__(setter, value)
      end
      arrow_options
    end
  end
end
