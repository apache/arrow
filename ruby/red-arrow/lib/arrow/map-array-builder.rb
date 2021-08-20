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
  class MapArrayBuilder
    class << self
      def build(data_type, values)
        builder = new(data_type)
        builder.build(values)
      end
    end

    alias_method :append_value_raw, :append_value

    # @overload append_value
    #
    #   Starts appending a map record. You need to append
    #   values of map by {#key_builder} and {#item_builder}.
    #
    # @overload append_value(value)
    #
    #   Appends a map record including key and item values.
    #
    #   @param value [nil, #each] The map record.
    #
    #     If this is `nil`, the map record is null.
    #
    #     If this is an `Object` that has `#each`, each value is a pair of key and item.
    #
    # @since 6.0.0
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
        else
          unless value.respond_to?(:each)
            message = "map value must be nil, Hash or Object that has #each: #{value.inspect}"
            raise ArgumentError, message
          end
          append_value_raw
          @key_builder ||= key_builder
          @item_builder ||= item_builder
          case value
          when Hash
            keys = value.keys
            values = value.values
          else
            keys = []
            values = []
            value.each do |key, item|
              keys << key
              values << item
            end
          end
          @key_builder.append(*keys)
          @item_builder.append(*values)
        end
      else
        message = "wrong number of arguments (given #{n_args}, expected 0..1)"
        raise ArgumentError, message
      end
    end

    alias_method :append_values_raw, :append_values

    def append_values(values, is_valids=nil)
      value = values[0]
      case value
      when Integer
        append_values_raw(values, is_valids)
      else
        if is_valids
          is_valids.each_with_index do |is_valid, i|
            if is_valid
              append_value(values[i])
            else
              append_null
            end
          end
        else
          values.each do |value|
            append_value(value)
          end
        end
      end
    end
  end
end
