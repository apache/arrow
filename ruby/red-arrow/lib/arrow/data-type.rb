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
  class DataType
    class << self
      # Creates a new suitable {Arrow::DataType}.
      #
      # @overload resolve(data_type)
      #
      #   Returns the given data type itself. This is convenient to
      #   use this method as {Arrow::DataType} converter.
      #
      #   @param data_type [Arrow::DataType] The data type.
      #
      #   @return [Arrow::DataType] The given data type itself.
      #
      # @overload resolve(name, *arguments)
      #
      #   Creates a suitable data type from type name. For example,
      #   you can create {Arrow::BooleanDataType} from `:boolean`.
      #
      #   @param name [String, Symbol] The type name of the data type.
      #
      #   @param arguments [::Array] The additional information of the
      #     data type.
      #
      #     For example, {Arrow::TimestampDataType} needs unit as
      #     additional information.
      #
      #   @example Create a boolean data type
      #     Arrow::DataType.resolve(:boolean)
      #
      #   @example Create a milliseconds unit timestamp data type
      #     Arrow::DataType.resolve(:timestamp, :milli)
      #
      # @overload resolve(description)
      #
      #   Creates a suitable data type from data type description.
      #
      #   Data type description is a raw `Hash`. Data type description
      #   must have `:type` value. `:type` is the type of the data type.
      #
      #   If the type needs additional information, you need to
      #   specify it. See constructor document what information is
      #   needed. For example, {Arrow::ListDataType#initialize} needs
      #   `:field` value.
      #
      #   @param description [Hash] The description of the data type.
      #
      #   @option description [String, Symbol] :type The type name of
      #     the data type.
      #
      #   @example Create a boolean data type
      #     Arrow::DataType.resolve(type: :boolean)
      #
      #   @example Create a list data type
      #     Arrow::DataType.resolve(type: :list,
      #                             field: {name: "visible", type: :boolean})
      def resolve(data_type)
        case data_type
        when DataType
          data_type
        when String, Symbol
          resolve_class(data_type).new
        when ::Array
          type, *arguments = data_type
          resolve_class(type).new(*arguments)
        when Hash
          type = nil
          description = {}
          data_type.each do |key, value|
            key = key.to_sym
            case key
            when :type
              type = value
            else
              description[key] = value
            end
          end
          if type.nil?
            message =
              "data type description must have :type value: #{data_type.inspect}"
            raise ArgumentError, message
          end
          data_type_class = resolve_class(type)
          if description.empty?
            data_type_class.new
          else
            data_type_class.new(description)
          end
        else
          message =
            "data type must be " +
            "Arrow::DataType, String, Symbol, [String, ...], [Symbol, ...] " +
            "{type: String, ...} or {type: Symbol, ...}: #{data_type.inspect}"
          raise ArgumentError, message
        end
      end

      private
      def resolve_class(data_type)
        data_type_name = data_type.to_s.capitalize.gsub(/\AUint/, "UInt")
        data_type_class_name = "#{data_type_name}DataType"
        unless Arrow.const_defined?(data_type_class_name)
          available_types = []
          Arrow.constants.each do |name|
            if name.to_s.end_with?("DataType")
              available_types << name.to_s.gsub(/DataType\z/, "").downcase.to_sym
            end
          end
          message =
            "unknown type: #{data_type.inspect}: " +
            "available types: #{available_types.inspect}"
          raise ArgumentError, message
        end
        Arrow.const_get(data_type_class_name)
      end
    end
  end
end
