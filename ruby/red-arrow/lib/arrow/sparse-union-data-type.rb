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
  class SparseUnionDataType
    alias_method :initialize_raw, :initialize
    private :initialize_raw

    # Creates a new {Arrow::SparseUnionDataType}.
    #
    # @overload initialize(fields, type_codes)
    #
    #   @param fields [::Array<Arrow::Field, Hash>] The fields of the
    #     sparse union data type. You can mix {Arrow::Field} and field
    #     description in the fields.
    #
    #     See {Arrow::Field.new} how to specify field description.
    #
    #   @param type_codes [::Array<Integer>] The IDs that indicates
    #     corresponding fields.
    #
    #   @example Create a sparse union data type for {2: visible, 9: count}
    #     fields = [
    #       Arrow::Field.new("visible", :boolean),
    #       {
    #         name: "count",
    #         type: :int32,
    #       },
    #     ]
    #     Arrow::SparseUnionDataType.new(fields, [2, 9])
    #
    # @overload initialize(description)
    #
    #   @param description [Hash] The description of the sparse union
    #     data type. It must have `:fields` and `:type_codes` values.
    #
    #   @option description [::Array<Arrow::Field, Hash>] :fields The
    #     fields of the sparse union data type. You can mix
    #     {Arrow::Field} and field description in the fields.
    #
    #     See {Arrow::Field.new} how to specify field description.
    #
    #   @option description [::Array<Integer>] :type_codes The IDs
    #     that indicates corresponding fields.
    #
    #   @example Create a sparse union data type for {2: visible, 9: count}
    #     fields = [
    #       Arrow::Field.new("visible", :boolean),
    #       {
    #         name: "count",
    #         type: :int32,
    #       },
    #     ]
    #     Arrow::SparseUnionDataType.new(fields: fields,
    #                                    type_codes: [2, 9])
    def initialize(*args)
      n_args = args.size
      case n_args
      when 1
        description = args[0]
        fields = description[:fields]
        type_codes = description[:type_codes]
      when 2
        fields, type_codes = args
      else
        message = "wrong number of arguments (given, #{n_args}, expected 1..2)"
        raise ArgumentError, message
      end
      fields = fields.collect do |field|
        field = Field.new(field) unless field.is_a?(Field)
        field
      end
      initialize_raw(fields, type_codes)
    end
  end
end
