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
    def initialize(schema)
      @schema = schema
    end

    def build
      builder = yield(Record.new(@schema), Context.new)
      node = builder.build
      Expression.new(node,
                     Arrow::Field.new("result", node.return_type))
    end
  end
end

require "gandiva/expression-builder/add"
require "gandiva/expression-builder/context"
require "gandiva/expression-builder/divide"
require "gandiva/expression-builder/elsif"
require "gandiva/expression-builder/equal"
require "gandiva/expression-builder/field"
require "gandiva/expression-builder/greater-than"
require "gandiva/expression-builder/if"
require "gandiva/expression-builder/literal"
require "gandiva/expression-builder/less-than"
require "gandiva/expression-builder/multiply"
require "gandiva/expression-builder/record"
require "gandiva/expression-builder/subtract"
