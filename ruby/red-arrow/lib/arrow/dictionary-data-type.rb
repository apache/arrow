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
  class DictionaryDataType
    alias_method :initialize_raw, :initialize
    private :initialize_raw

    # Creates a new {Arrow::DictionaryDataType}.
    #
    # @overload initialize(index_data_type, value_data_type, ordered)
    #
    #   @param index_data_type [Arrow::DataType, Hash, String, Symbol]
    #     The index data type of the dictionary data type. It must be
    #     signed integer data types. Here are available signed integer
    #     data types:
    #
    #       * Arrow::Int8DataType
    #       * Arrow::Int16DataType
    #       * Arrow::Int32DataType
    #       * Arrow::Int64DataType
    #
    #     You can specify data type as a description by `Hash`.
    #
    #     See {Arrow::DataType.resolve} how to specify data type
    #     description.
    #
    #   @param value_data_type [Arrow::DataType, Hash, String, Symbol]
    #     The value data type of the dictionary data type.
    #
    #     You can specify data type as a description by `Hash`.
    #
    #     See {Arrow::DataType.resolve} how to specify data type
    #     description.
    #
    #   @param ordered [Boolean] Whether dictionary contents are
    #     ordered or not.
    #
    #   @example Create a dictionary data type for {0: "Hello", 1: "World"}
    #     index_data_type = :int8
    #     value_data_type = :string
    #     ordered = true
    #     Arrow::DictionaryDataType.new(index_data_type,
    #                                   value_data_type,
    #                                   ordered)
    #
    # @overload initialize(description)
    #
    #   @param description [Hash] The description of the dictionary
    #     data type. It must have `:index_data_type`, `:dictionary`
    #     and `:ordered` values.
    #
    #   @option description [Arrow::DataType, Hash, String, Symbol]
    #     :index_data_type The index data type of the dictionary data
    #     type. It must be signed integer data types. Here are
    #     available signed integer data types:
    #
    #       * Arrow::Int8DataType
    #       * Arrow::Int16DataType
    #       * Arrow::Int32DataType
    #       * Arrow::Int64DataType
    #
    #     You can specify data type as a description by `Hash`.
    #
    #     See {Arrow::DataType.resolve} how to specify data type
    #     description.
    #
    #   @option description [Arrow::DataType, Hash, String, Symbol]
    #     :value_data_type
    #     The value data type of the dictionary data type.
    #
    #     You can specify data type as a description by `Hash`.
    #
    #     See {Arrow::DataType.resolve} how to specify data type
    #     description.
    #
    #   @option description [Boolean] :ordered Whether dictionary
    #     contents are ordered or not.
    #
    #   @example Create a dictionary data type for {0: "Hello", 1: "World"}
    #     Arrow::DictionaryDataType.new(index_data_type: :int8,
    #                                   value_data_type: :string,
    #                                   ordered: true)
    def initialize(*args)
      n_args = args.size
      case n_args
      when 1
        description = args[0]
        index_data_type = description[:index_data_type]
        value_data_type = description[:value_data_type]
        ordered = description[:ordered]
      when 3
        index_data_type, value_data_type, ordered = args
      else
        message = "wrong number of arguments (given, #{n_args}, expected 1 or 3)"
        raise ArgumentError, message
      end
      index_data_type = DataType.resolve(index_data_type)
      value_data_type = DataType.resolve(value_data_type)
      initialize_raw(index_data_type, value_data_type, ordered)
    end
  end
end
