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
  class Scalar
    class << self
      # @api private
      def try_convert(value)
        case value
        when self
          value
        when true, false
          BooleanScalar.new(value)
        when Symbol, String
          StringScalar.new(value.to_s)
        when Integer
          Int64Scalar.new(value)
        when Float
          DoubleScalar.new(value)
        else
          nil
        end
      end

      # Ensure returning suitable {Arrow::Scalar}.
      #
      # @overload resolve(scalar)
      #
      #   Returns the given scalar itself. This is convenient to
      #   use this method as {Arrow::Scalar} converter.
      #
      #   @param scalar [Arrow::Scalar] The scalar.
      #
      #   @return [Arrow::Scalar] The given scalar itself.
      #
      # @overload resolve(value)
      #
      #   Creates a suitable scalar from the given value. For example,
      #   you can create {Arrow::BooleanScalar} from `true`.
      #
      #   @param value [Object] The value.
      #
      #   @return [Arrow::Scalar] A suitable {Arrow::Scalar} for `value`.
      #
      # @overload resolve(value, data_type)
      #
      #   Creates a scalar of `data_type.scalar_class` from the given
      #   value. For example, you can create {Arrow::Int32Scalar} from
      #   `29` and {Arrow::Int32DataType}.
      #
      #   @param value [Object] The value.
      #
      #   @param data_type [Arrow::DataType] The {Arrow::DataType} to
      #     decide the returned scalar class.
      #
      #   @return [Arrow::Scalar] A suitable {Arrow::Scalar} for `value`.
      #
      # @since 12.0.0
      def resolve(value, data_type=nil)
        return try_convert(value) if data_type.nil?

        data_type = DataType.resolve(data_type)
        scalar_class = data_type.scalar_class
        case value
        when Scalar
          return value if value.class == scalar_class
          value = value.value
        end
        scalar_class.new(value)
      end
    end

    # @param other [Arrow::Scalar] The scalar to be compared.
    # @param options [Arrow::EqualOptions, Hash] (nil)
    #   The options to custom how to compare.
    #
    # @return [Boolean]
    #   `true` if both of them have the same data, `false` otherwise.
    #
    # @since 5.0.0
    def equal_scalar?(other, options=nil)
      equal_options(other, options)
    end
  end
end
