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
  class ListDataType
    alias_method :initialize_raw, :initialize
    private :initialize_raw

    # Creates a new {Arrow::ListDataType}.
    #
    # @overload initialize(field)
    #
    #   @param field [Arrow::Field, Hash] The field of the list data
    #     type. You can also specify field description by `Hash`.
    #
    #     See {Arrow::Field.new} how to specify field description.
    #
    #   @example Create a list data type with {Arrow::Field}
    #     visible_field = Arrow::Field.new("visible", :boolean)
    #     Arrow::ListDataType.new(visible_field)
    #
    #   @example Create a list data type with field description
    #     Arrow::ListDataType.new(name: "visible", type: :boolean)
    #
    # @overload initialize(description)
    #
    #   @param description [Hash] The description of the list data
    #     type. It must have `:field` value.
    #
    #   @option description [Arrow::Field, Hash] :field The field of
    #     the list data type. You can also specify field description
    #     by `Hash`.
    #
    #     See {Arrow::Field.new} how to specify field description.
    #
    #   @example Create a list data type with {Arrow::Field}
    #     visible_field = Arrow::Field.new("visible", :boolean)
    #     Arrow::ListDataType.new(field: visible_field)
    #
    #   @example Create a list data type with field description
    #     Arrow::ListDataType.new(field: {name: "visible", type: :boolean})
    def initialize(field)
      if field.is_a?(Hash) and field.key?(:field)
        description = field
        field = description[:field]
      end
      if field.is_a?(Hash)
        field_description = field
        field = Field.new(field_description)
      end
      initialize_raw(field)
    end
  end
end
