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

require "gandiva/expression-builder/value"

module Gandiva
  class ExpressionBuilder
    class BinaryOperation < Value
      def initialize(operator, left, right)
        @operator = operator
        @left = left
        @right = right
      end

      def build
        result = Arrow::Field.new("result", node.return_type)
        Gandiva::Expression.new(node, result)
      end

      def node
        @node ||= Gandiva::FunctionNode.new(@operator,
                                            [@left.node, @right.node],
                                            return_type)
      end

      private
      def return_type
        if ["greater_than", "less_than", "equal"].include?(@operator)
          Arrow::BooleanDataType.new
        else
          @right.node.return_type
        end
      end
    end
  end
end
