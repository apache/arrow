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

module Gandiva
  class ExpressionBuilder
    class Literal
      class << self
        def resolve(value)
          case value
          when true, false
            new(BooleanLiteralNode, value)
          when Integer
            if value < -(2 ** 31)
              new(Int64LiteralNode, value)
            elsif value < -(2 ** 15)
              new(Int32LiteralNode, value)
            elsif value < -(2 ** 7)
              new(Int16LiteralNode, value)
            elsif value < 0
              new(Int8LiteralNode, value)
            elsif value < (2 ** 8 - 1)
              new(UInt8LiteralNode, value)
            elsif value < (2 ** 16 - 1)
              new(UInt16LiteralNode, value)
            elsif value < (2 ** 32 - 1)
              new(UInt32LiteralNode, value)
            else
              new(UInt64LiteralNode, value)
            end
          when Float
            new(DoubleLiteralNode, value)
          when String
            new(StringLiteralNode, value)
          else
            nil
          end
        end
      end

      attr_reader :value
      def initialize(node_class, value)
        @node_class = node_class
        @value = value
      end

      def build
        @node_class.new(value)
      end
    end
  end
end
