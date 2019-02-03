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
  class Schema
    include FieldContainable

    alias_method :initialize_raw, :initialize
    private :initialize_raw

    # Creates a new {Arrow::Schema}.
    #
    # @overload initialize(fields)
    #
    #   @param fields [::Array<Arrow::Field, Hash>] The fields of the
    #     schema. You can mix {Arrow::Field} and field description in
    #     the fields.
    #
    #     See {Arrow::Field.new} how to specify field description.
    #
    #   @example Create a schema with {Arrow::Field}s
    #     visible_field = Arrow::Field.new("visible", :boolean)
    #     Arrow::Schema.new([visible_field])
    #
    #   @example Create a schema with field descriptions
    #      visible_field_description = {
    #        name: "visible",
    #        data_type: :boolean,
    #      }
    #      Arrow::Schema.new([visible_field_description])
    #
    #   @example Create a schema with {Arrow::Field}s and field descriptions
    #      fields = [
    #        Arrow::Field.new("visible", :boolean),
    #        {
    #          name: "count",
    #          type: :int32,
    #        },
    #      ]
    #      Arrow::Schema.new(fields)
    #
    # @overload initialize(fields)
    #
    #   @param fields [Hash{String, Symbol => Arrow::DataType, Hash}]
    #     The pairs of field name and field data type of the schema.
    #     You can mix {Arrow::DataType} and data description for field
    #     data type.
    #
    #     See {Arrow::DataType.new} how to specify data type description.
    #
    #   @example Create a schema with fields
    #      fields = {
    #        "visible" => Arrow::BooleanDataType.new,
    #        :count => :int32,
    #        :tags => {
    #          type: :list,
    #          field: {
    #            name: "tag",
    #            type: :string,
    #          },
    #        },
    #      }
    #      Arrow::Schema.new(fields)
    def initialize(fields)
      case fields
      when ::Array
        fields = fields.collect do |field|
          field = Field.new(field) unless field.is_a?(Field)
          field
        end
      when Hash
        fields = fields.collect do |name, data_type|
          Field.new(name, data_type)
        end
      end
      initialize_raw(fields)
    end

    alias_method :[], :find_field
  end
end
