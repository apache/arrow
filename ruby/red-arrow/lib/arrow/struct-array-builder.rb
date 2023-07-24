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
  class StructArrayBuilder
    class << self
      def build(data_type, values)
        builder = new(data_type)
        builder.build(values)
      end
    end

    def [](index_or_name)
      find_field_builder(index_or_name)
    end

    def find_field_builder(index_or_name)
      case index_or_name
      when String, Symbol
        name = index_or_name
        cached_name_to_builder[name.to_s]
      else
        index = index_or_name
        cached_field_builders[index]
      end
    end

    alias_method :append_value_raw, :append_value

    # @overload append_value
    #
    #   Starts appending a struct record. You need to append values of
    #   fields.
    #
    # @overload append_value(value)
    #
    #   Appends a struct record including values of fields.
    #
    #   @param value [nil, ::Array, Hash] The struct record value.
    #
    #     If this is `nil`, the struct record is null.
    #
    #     If this is `Array` or `Hash`, they are values of fields.
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
          cached_field_builders.zip(value) do |builder, sub_value|
            builder.append(sub_value)
          end
        when Hash
          append_value_raw
          local_name_to_builder = cached_name_to_builder.dup
          value.each do |name, sub_value|
            builder = local_name_to_builder.delete(name.to_s)
            builder.append(sub_value)
          end
          local_name_to_builder.each do |_, builder|
            builder.append_null
          end
        else
          message =
            "struct value must be nil, Array or Hash: #{value.inspect}"
          raise ArgumentError, message
        end
      else
        message = "wrong number of arguments (given #{n_args}, expected 0..1)"
        raise ArgumentError, message
      end
    end

    def append_values(values, is_valids=nil)
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

    # @since 0.12.0
    def append(*values)
      if values.empty?
        # For backward compatibility
        append_value_raw
      else
        super
      end
    end

    private
    def cached_field_builders
      @field_builders ||= field_builders
    end

    def build_name_to_builder
      name_to_builder = {}
      builders = cached_field_builders
      value_data_type.fields.each_with_index do |field, i|
        name_to_builder[field.name] = builders[i]
      end
      name_to_builder
    end

    def cached_name_to_builder
      @name_to_builder ||= build_name_to_builder
    end
  end
end
