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
    class If
      def initialize(condition)
        @condition = condition
        @then = nil
        @else = nil
      end

      def then(clause)
        @then = clause
        self
      end

      def else(clause)
        @else = clause
        self
      end

      def elsif(condition)
        Elsif.new(self, condition)
      end

      def build
        build_if_node(condition_node,
                      then_node,
                      else_node)
      end

      protected
      def condition_node
        @condition.build
      end

      def then_node
        @then&.build
      end

      def else_node
        @else&.build
      end

      private
      def build_if_node(condition_node, then_node, else_node)
        if then_node and else_node
          # TODO: Validate then_node.return_type == else_node.return_type
          return_type = then_node.return_type
        else
          return_type = (then_node || else_node).return_type
        end
        IfNode.new(condition_node,
                   then_node,
                   else_node,
                   return_type)
      end
    end
  end
end
