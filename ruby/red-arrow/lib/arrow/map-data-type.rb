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
  class MapDataType
    alias_method :initialize_raw, :initialize
    private :initialize_raw

    # Creates a new {Arrow::MapDataType}.
    #
    # @overload initialize(key_type, item_type)
    #
    #   @param key_type [Arrow::DataType, Hash, String, Symbol]
    #     The key data type of the map data type.
    #
    #     You can specify data type as a description by `Hash`.
    #
    #     See {Arrow::DataType.resolve} how to specify data type
    #     description.
    #
    #   @param item_type [Arrow::DataType, Hash, String, Symbol]
    #     The item data type of the map data type.
    #
    #     You can specify data type as a description by `Hash`.
    #
    #     See {Arrow::DataType.resolve} how to specify data type
    #     description.
    #
    #   @example Create a map data type for {0: "Hello", 1: "World"}
    #     key_type = :int8
    #     item_type = :string
    #     Arrow::MapDataType.new(key_type, item_type)
    #
    # @overload initialize(description)
    #
    #   @param description [Hash] The description of the map data
    #     type. It must have `:key_type`, `:item_type` values.
    #
    #   @option description [Arrow::DataType, Hash, String, Symbol]
    #     :key_type The key data type of the map data type.
    #
    #     You can specify data type as a description by `Hash`.
    #
    #     See {Arrow::DataType.resolve} how to specify data type
    #     description.
    #
    #   @option description [Arrow::DataType, Hash, String, Symbol]
    #     :item_type  The item data type of the map data type.
    #
    #     You can specify data type as a description by `Hash`.
    #
    #     See {Arrow::DataType.resolve} how to specify data type
    #     description.
    #
    #   @example Create a maap data type for {0: "Hello", 1: "World"}
    #     Arrow::MapDataType.new(key_type: :int8, item_type: :string)
    def initialize(*args)
      n_args = args.size
      case n_args
      when 1
        description = args[0]
        key_type = description[:key_type]
        item_type = description[:item_type]
      when 2
        key_type, item_type = args
      else
        message = "wrong number of arguments (given, #{n_args}, expected 1..2)"
        raise ArgumentError, message
      end
      key_type = DataType.resolve(key_type)
      item_type = DataType.resolve(item_type)
      initialize_raw(key_type, item_type)
    end
  end
end
