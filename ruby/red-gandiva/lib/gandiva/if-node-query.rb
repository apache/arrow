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
  class IfNodeQuery
    def initialize(condition)
      @condition_nodes = [condition]
      @then_nodes = []
    end

    def then(node)
      @then_nodes << node
      self
    end

    def elsif(node)
      @condition_nodes << node
      self
    end

    def else(node)
      to_node(node)
    end

    private

    def to_node(node)
      node_size = @condition_nodes.size - 1
      (0..node_size).reverse_each do |i|
        node = build_if_node(node, i)
      end
      node
    end

    def build_if_node(node, i)
      Gandiva::IfNode.new(@condition_nodes[i],
                          @then_nodes[i],
                          node,
                          node.return_type)
    end
  end
end
