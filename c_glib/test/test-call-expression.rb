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

class TestCallExpression < Test::Unit::TestCase
  def setup
    @arguments = [
      Arrow::FieldExpression.new("augend"),
      Arrow::FieldExpression.new("addend"),
    ]
    @expression = Arrow::CallExpression.new("add", @arguments)
  end

  sub_test_case("==") do
    def test_true
      assert_equal(Arrow::CallExpression.new("now", []),
                   Arrow::CallExpression.new("now", []))
    end

    def test_false
      assert_not_equal(Arrow::CallExpression.new("a", []),
                       Arrow::CallExpression.new("b", []))
    end
  end

  def test_to_string
    assert_equal("add(augend, addend)", @expression.to_s)
  end
end
