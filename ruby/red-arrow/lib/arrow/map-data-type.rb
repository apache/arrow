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
    # @overload initialize(key, item)
    #
    #   @param key [Arrow::DataType, Hash, String, Symbol]
    #     The key data type of the map data type.
    #
    #     You can specify data type as a description by `Hash`.
    #
    #     See {Arrow::DataType.resolve} how to specify data type
    #     description.
    #
    #   @param item [Arrow::DataType, Hash, String, Symbol]
    #     The item data type of the map data type.
    #
    #     You can specify data type as a description by `Hash`.
    #
    #     See {Arrow::DataType.resolve} how to specify data type
    #     description.
    #
    #   @example Create a map data type for `{0: "Hello", 1: "World"}`
    #     key = :int8
    #     item = :string
    #     Arrow::MapDataType.new(key, item)
    #
    # @overload initialize(description)
    #
    #   @param description [Hash] The description of the map data
    #     type. It must have `:key`, `:item` values.
    #
    #   @option description [Arrow::DataType, Hash, String, Symbol]
    #     :key The key data type of the map data type.
    #
    #     You can specify data type as a description by `Hash`.
    #
    #     See {Arrow::DataType.resolve} how to specify data type
    #     description.
    #
    #   @option description [Arrow::DataType, Hash, String, Symbol]
    #     :item  The item data type of the map data type.
    #
    #     You can specify data type as a description by `Hash`.
    #
    #     See {Arrow::DataType.resolve} how to specify data type
    #     description.
    #
    #   @example Create a map data type for `{0: "Hello", 1: "World"}`
    #     Arrow::MapDataType.new(key: :int8, item: :string)
    def initialize(*args)
      n_args = args.size
      case n_args
      when 1
        description = args[0]
        key = description[:key]
        item = description[:item]
      when 2
        key, item = args
      else
        message = "wrong number of arguments (given, #{n_args}, expected 1..2)"
        raise ArgumentError, message
      end
      key = DataType.resolve(key)
      item = DataType.resolve(item)
      initialize_raw(key, item)
    end
  end
end
