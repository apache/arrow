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
  class Decimal128DataType
    alias_method :initialize_raw, :initialize
    private :initialize_raw

    # Creates a new {Arrow::Decimal128DataType}.
    #
    # @overload initialize(precision, scale)
    #
    #   @param precision [Integer] The precision of the decimal data
    #     type. It's the number of digits including the number of
    #     digits after the decimal point.
    #
    #   @param scale [Integer] The scale of the decimal data
    #     type. It's the number of digits after the decimal point.
    #
    #   @example Create a decimal data type for "XXXXXX.YY" decimal
    #     Arrow::Decimal128DataType.new(8, 2)
    #
    # @overload initialize(description)
    #
    #   @param description [Hash] The description of the decimal data
    #     type. It must have `:precision` and `:scale` values.
    #
    #   @option description [Integer] :precision The precision of the
    #     decimal data type. It's the number of digits including the
    #     number of digits after the decimal point.
    #
    #   @option description [Integer] :scale The scale of the decimal
    #     data type. It's the number of digits after the decimal
    #     point.
    #
    #   @example Create a decimal data type for "XXXXXX.YY" decimal
    #     Arrow::Decimal128DataType.new(precision: 8,
    #                                   scale: 2)
    def initialize(*args)
      n_args = args.size
      case n_args
      when 1
        description = args[0]
        precision = description[:precision]
        scale = description[:scale]
      when 2
        precision, scale = args
      else
        message = "wrong number of arguments (given, #{n_args}, expected 1..2)"
        raise ArgumentError, message
      end
      initialize_raw(precision, scale)
    end
  end
end
