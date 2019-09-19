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
  class FieldNode
    def +(node)
      build_function_node("add", node)
    end

    def -(node)
      build_function_node("subtract", node)
    end

    def *(node)
      build_function_node("multiply", node)
    end

    def /(node)
      build_function_node("divide", node)
    end

    def >(node)
      build_function_node("greater_than", node)
    end

    def <(node)
      build_function_node("less_than", node)
    end

    def ==(node)
      build_function_node("equal", node)
    end

    private

    def build_function_node(operator, node)
      return_type =
        if (operator == "greater_than" ||
          operator == "less_than" || operator == "equal")
          Arrow::BooleanDataType.new
        else
          field.data_type
        end

      Gandiva::FunctionNode.new(operator,
                                [self, node],
                                return_type)
    end
  end
end
