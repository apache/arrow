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
  class ListArrayBuilder
    class << self
      def build(data_type, values)
        builder = new(data_type)
        builder.build(values)
      end
    end

    alias_method :append_value_raw, :append_value

    # @overload append_value
    #
    #   Starts appending a list record. You also need to append list
    #   value by {#value_builder}.
    #
    # @overload append_value(list)
    #
    #   Appends a list record including list value.
    #
    #   @param value [nil, ::Array] The list value of the record.
    #
    #     If this is `nil`, the list record is null.
    #
    #     If this is `Array`, it's the list value of the record.
    #
    # @since 0.12.0
    def append_value(*args)
      n_args = args.size

      case n_args
      when 0
        append_value_raw
      when 1
        value = args[0]
        case value
        when nil
          append_null
        when ::Array
          append_value_raw
          @value_builder ||= value_builder
          @value_builder.append_values(value, nil)
        else
          message = "list value must be nil or Array: #{value.inspect}"
          raise ArgumentError, message
        end
      else
        message = "wrong number of arguments (given #{n_args}, expected 0..1)"
        raise ArgumentError, message
      end
    end

    def append_values(lists, is_valids=nil)
      if is_valids
        is_valids.each_with_index do |is_valid, i|
          if is_valid
            append_value(lists[i])
          else
            append_null
          end
        end
      else
        lists.each do |list|
          append_value(list)
        end
      end
    end

    # @since 0.12.0
    def append(*values)
      if values.empty?
        # For backward compatibility
        append_value
      else
        super
      end
    end
  end
end
