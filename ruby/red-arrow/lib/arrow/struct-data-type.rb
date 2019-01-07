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

require "arrow/field-containable"

module Arrow
  class StructDataType
    include FieldContainable

    alias_method :initialize_raw, :initialize
    private :initialize_raw

    # Creates a new {Arrow::StructDataType}.
    #
    # @overload initialize(fields)
    #
    #   @param fields [::Array<Arrow::Field, Hash>] The fields of the
    #     struct data type. You can also specify field description as
    #     a field. You can mix {Arrow::Field} and field description.
    #
    #     See {Arrow::Field.new} how to specify field description.
    #
    #   @example Create a struct data type with {Arrow::Field}s
    #     visible_field = Arrow::Field.new("visible", :boolean)
    #     count_field = Arrow::Field.new("count", :int32)
    #     Arrow::StructDataType.new([visible_field, count_field])
    #
    #   @example Create a struct data type with field descriptions
    #     field_descriptions = [
    #       {name: "visible", type: :boolean},
    #       {name: "count", type: :int32},
    #     ]
    #     Arrow::StructDataType.new(field_descriptions)
    #
    #   @example Create a struct data type with {Arrow::Field} and field description
    #     fields = [
    #       Arrow::Field.new("visible", :boolean),
    #       {name: "count", type: :int32},
    #     ]
    #     Arrow::StructDataType.new(fields)
    #
    # @overload initialize(fields)
    #
    #   @param fields [Hash{String, Symbol => Arrow::DataType, Hash}]
    #     The pairs of field name and field data type of the struct
    #     data type. You can also specify data type description by
    #     `Hash`. You can mix {Arrow::DataType} and data type description.
    #
    #     See {Arrow::DataType.resolve} how to specify data type
    #     description.
    #
    #   @example Create a struct data type with {Arrow::DataType}s
    #     fields = {
    #       "visible" => Arrow::BooleanDataType.new,
    #       "count" => Arrow::Int32DataType.new,
    #     }
    #     Arrow::StructDataType.new(fields)
    #
    #   @example Create a struct data type with data type descriptions
    #     fields = {
    #       "visible" => :boolean,
    #       "count" => {type: :int32},
    #     }
    #     Arrow::StructDataType.new(fields)
    #
    #   @example Create a struct data type with {Arrow::DataType} and data type description
    #     fields = {
    #       "visible" => Arrow::BooleanDataType.new,
    #       "count" => {type: :int32},
    #     }
    #     Arrow::StructDataType.new(fields)
    #
    # @overload initialize(description)
    #
    #   @param description [Hash] The description of the struct data
    #     type. It must have `:fields` value.
    #
    #   @option description
    #     [::Array<Arrow::Field, Hash>,
    #      Hash{String, Symbol => Arrow::DataType, Hash, String, Symbol}]
    #     :fields The fields of the struct data type.
    #
    #   @example Create a struct data type with {Arrow::Field} and field description
    #     fields = [
    #       Arrow::Field.new("visible", :boolean),
    #       {name: "count", type: :int32},
    #     ]
    #     Arrow::StructDataType.new(fields: fields)
    #
    #   @example Create a struct data type with {Arrow::DataType} and data type description
    #     fields = {
    #       "visible" => Arrow::BooleanDataType.new,
    #       "count" => {type: :int32},
    #     }
    #     Arrow::StructDataType.new(fields: fields)
    def initialize(fields)
      if fields.is_a?(Hash) and fields.key?(:fields)
        description = fields
        fields = description[:fields]
      end
      if fields.is_a?(Hash)
        fields = fields.collect do |name, data_type|
          Field.new(name, data_type)
        end
      else
        fields = fields.collect do |field|
          field = Field.new(field) unless field.is_a?(Field)
          field
        end
      end
      initialize_raw(fields)
    end

    alias_method :[], :find_field
  end
end
