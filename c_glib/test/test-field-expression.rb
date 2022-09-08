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

class TestFieldExpression < Test::Unit::TestCase
  def setup
    @expression = Arrow::FieldExpression.new("visible")
  end

  sub_test_case("#initialize") do
    def test_invalid_dot_path
      message =
        "[field-expression][new]: Invalid: " +
        "Dot path '.[' contained an unterminated index"
      assert_raise(Arrow::Error::Invalid.new(message)) do
        Arrow::FieldExpression.new(".[")
      end
    end
  end

  sub_test_case("==") do
    def test_true
      assert_equal(Arrow::FieldExpression.new("visible"),
                   Arrow::FieldExpression.new(".visible"))
    end

    def test_false
      assert_not_equal(@expression,
                       Arrow::FieldExpression.new("equal"))
    end
  end

  def test_to_string
    assert_equal("visible", @expression.to_s)
  end
end
