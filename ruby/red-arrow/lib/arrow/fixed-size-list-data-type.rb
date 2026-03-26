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
  class FixedSizeListDataType
    include ListFieldResolvable

    alias_method :initialize_raw, :initialize
    private :initialize_raw

    # Creates a new {Arrow::FixedSizeListDataType}.
    #
    # @overload initialize(field, size)
    #
    #   @param field [Arrow::Field, Hash] The field of the fixed size
    #     list data type. You can also specify field description by
    #     `Hash`.
    #
    #     See {Arrow::Field.new} how to specify field description.
    #
    #   @param size [Integer] The number of values in each element.
    #
    #   @example Create a fixed size list data type with {Arrow::Field}
    #     visible_field = Arrow::Field.new("visible", :boolean)
    #     size = 2
    #     Arrow::FixedSizeListDataType.new(visible_field, size)
    #
    #   @example Create a list data type with field description
    #     description = {name: "visible", type: :boolean}
    #     size = 2
    #     Arrow::FixedSizeListDataType.new(description, size)
    #
    # @overload initialize(description)
    #
    #   @param description [Hash] The description of the fixed size
    #     list data type. It must have `:field` value and `:size`
    #     value.
    #
    #   @option description [Arrow::Field, Hash] :field The field of
    #     the list data type. You can also specify field description
    #     by `Hash`.
    #
    #     See {Arrow::Field.new} how to specify field description.
    #
    #   @option description [Integer] :size The number of values of
    #     each element of the fixed size list data type.
    #
    #   @example Create a fixed size list data type with {Arrow::Field}
    #     visible_field = Arrow::Field.new("visible", :boolean)
    #     Arrow::FixedSizeListDataType.new(field: visible_field, size: 2)
    #
    #   @example Create a fixed size list data type with field description
    #     Arrow::FixedSizeListDataType.new(field: {
    #                                        name: "visible",
    #                                        type: :boolean,
    #                                      },
    #                                      size: 2)
    #
    # @overload initialize(data_type, size)
    #
    #   @param data_type [Arrow::DataType, String, Symbol,
    #     ::Array<String>, ::Array<Symbol>, Hash] The element data
    #     type of the fixed size list data type. A field is created
    #     with the default name `"item"` from the data type
    #     automatically.
    #
    #     See {Arrow::DataType.resolve} how to specify data type.
    #
    #   @param size [Integer] The number of values in each
    #     element.
    #
    #   @example Create a fixed size list data type with {Arrow::DataType}
    #     size = 2
    #     Arrow::FixedSizeListDataType.new(Arrow::BooleanDataType.new,
    #                                      size)
    #
    #   @example Create a fixed size list data type with data type name as String
    #     size = 2
    #     Arrow::FixedSizeListDataType.new("boolean", size)
    #
    #   @example Create a fixed size list data type with data type name as Symbol
    #     size = 2
    #     Arrow::FixedSizeListDataType.new(:boolean, size)
    #
    #   @example Create a fixed size list data type with data type as Array
    #     size = 2
    #     Arrow::FixedSizeListDataType.new([:time32, :milli], size)
    def initialize(*args)
      n_args = args.size
      case n_args
      when 1
        description = args[0]
        size = description.delete(:size)
        initialize_raw(resolve_field(description), size)
      when 2
        field, size = args
        initialize_raw(resolve_field(field), size)
      else
        message = "wrong number of arguments (given #{n_args}, expected 1..2)"
        raise ArgumentError, message
      end
    end
  end
end
