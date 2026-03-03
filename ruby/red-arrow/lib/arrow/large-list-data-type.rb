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
  class LargeListDataType
    include ListFieldResolvable

    alias_method :initialize_raw, :initialize
    private :initialize_raw

    # Creates a new {Arrow::LargeListDataType}.
    #
    # @overload initialize(field)
    #
    #   @param field [Arrow::Field, Hash] The field of the large list
    #     data type. You can also specify field description by `Hash`.
    #
    #     See {Arrow::Field.new} how to specify field description.
    #
    #   @example Create a large list data type with {Arrow::Field}
    #     visible_field = Arrow::Field.new("visible", :boolean)
    #     Arrow::LargeListDataType.new(visible_field)
    #
    #   @example Create a large list data type with field description
    #     Arrow::LargeListDataType.new(name: "visible", type: :boolean)
    #
    # @overload initialize(description)
    #
    #   @param description [Hash] The description of the large list data
    #     type. It must have `:field` value.
    #
    #   @option description [Arrow::Field, Hash] :field The field of
    #     the large list data type. You can also specify field
    #     description by `Hash`.
    #
    #     See {Arrow::Field.new} how to specify field description.
    #
    #   @example Create a large list data type with {Arrow::Field}
    #     visible_field = Arrow::Field.new("visible", :boolean)
    #     Arrow::LargeListDataType.new(field: visible_field)
    #
    #   @example Create a large list data type with field description
    #     Arrow::LargeListDataType.new(field: {name: "visible", type: :boolean})
    #
    # @overload initialize(data_type)
    #
    #   @param data_type [Arrow::DataType, String, Symbol,
    #     ::Array<String>, ::Array<Symbol>, Hash] The element data
    #     type of the large list data type. A field is created with the
    #     default name `"item"` from the data type automatically.
    #
    #     See {Arrow::DataType.resolve} how to specify data type.
    #
    #   @example Create a large list data type with {Arrow::DataType}
    #     Arrow::LargeListDataType.new(Arrow::BooleanDataType.new)
    #
    #   @example Create a large list data type with data type name as String
    #     Arrow::LargeListDataType.new("boolean")
    #
    #   @example Create a large list data type with data type name as Symbol
    #     Arrow::LargeListDataType.new(:boolean)
    #
    #   @example Create a large list data type with data type as Array
    #     Arrow::LargeListDataType.new([:time32, :milli])
    def initialize(arg)
      initialize_raw(resolve_field(arg))
    end
  end
end
