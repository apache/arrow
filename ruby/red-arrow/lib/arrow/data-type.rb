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
      # Ensure returning suitable {Arrow::DataType}.
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
      # @overload resolve(name)
      #
      #   Creates a suitable data type from the given type name. For
      #   example, you can create {Arrow::BooleanDataType} from
      #   `:boolean`.
      #
      #   @param name [String, Symbol] The type name of the data type.
      #
      #   @return [Arrow::DataType] A new suitable data type.
      #
      #   @example Create a boolean data type
      #     Arrow::DataType.resolve(:boolean)
      #
      # @overload resolve(name_with_arguments)
      #
      #   Creates a new suitable data type from the given type name
      #   with arguments.
      #
      #   @param name_with_arguments [::Array<String, ...>]
      #     The type name of the data type as the first element.
      #
      #     The rest elements are additional information of the data type.
      #
      #     For example, {Arrow::TimestampDataType} needs unit as
      #     additional information.
      #
      #   @return [Arrow::DataType] A new suitable data type.
      #
      #   @example Create a boolean data type
      #     Arrow::DataType.resolve([:boolean])
      #
      #   @example Create a milliseconds unit timestamp data type
      #     Arrow::DataType.resolve([:timestamp, :milli])
      #
      # @overload resolve(description)
      #
      #   Creates a new suitable data type from the given data type
      #   description.
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
      #   @return [Arrow::DataType] A new suitable data type.
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
          if type.nil? and self == DataType
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

      def sub_types
        types = {}
        gtype.children.each do |child|
          sub_type = child.to_class
          types[sub_type] = true
          sub_type.sub_types.each do |sub_sub_type|
            types[sub_sub_type] = true
          end
        end
        types.keys
      end

      def try_convert(value)
        begin
          resolve(value)
        rescue ArgumentError
          nil
        end
      end

      private
      def resolve_class(data_type)
        return self if data_type.nil?
        components = data_type.to_s.split("_").collect(&:capitalize)
        data_type_name = components.join.gsub(/\AUint/, "UInt")
        data_type_class_name = "#{data_type_name}DataType"
        unless Arrow.const_defined?(data_type_class_name)
          available_types = []
          Arrow.constants.each do |name|
            name = name.to_s
            next if name == "DataType"
            next unless name.end_with?("DataType")
            name = name.gsub(/DataType\z/, "")
            components = name.scan(/(UInt[0-9]+|[A-Z][a-z\d]+)/).flatten
            available_types << components.collect(&:downcase).join("_").to_sym
          end
          message =
            "unknown type: <#{data_type.inspect}>: " +
            "available types: #{available_types.inspect}"
          raise ArgumentError, message
        end
        data_type_class = Arrow.const_get(data_type_class_name)
        if data_type_class.gtype.abstract?
          not_abstract_types = data_type_class.sub_types.find_all do |sub_type|
            not sub_type.gtype.abstract?
          end
          not_abstract_types = not_abstract_types.sort_by do |type|
            type.name
          end
          message =
            "abstract type: <#{data_type.inspect}>: " +
            "use one of not abstract type: #{not_abstract_types.inspect}"
          raise ArgumentError, message
        end
        data_type_class
      end
    end

    def array_class
      base_name = self.class.name.gsub(/DataType\z/, "")
      ::Arrow.const_get("#{base_name}Array")
    end

    def build_array(values)
      builder_class = array_class.builder_class
      args = [values]
      args.unshift(self) unless builder_class.buildable?(args)
      builder_class.build(*args)
    end

    # @return [Arrow::Scalar} A corresponding {Arrow::Scalar} class
    #   for this data type.
    #
    # @since 12.0.0
    def scalar_class
      base_name = self.class.name.gsub(/DataType\z/, "")
      ::Arrow.const_get("#{base_name}Scalar")
    end
  end
end
