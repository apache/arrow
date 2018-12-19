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

class TestGandivaFieldNode < Test::Unit::TestCase
  def setup
    omit("Gandiva is required") unless defined?(::Gandiva)
    @field = Arrow::Field.new("valid", Arrow::BooleanDataType.new)
    @node = Gandiva::FieldNode.new(@field)
  end

  def test_field
    assert_equal(@field, @node.field)
  end

  def test_return_type
    assert_equal(@field.data_type, @node.return_type)
  end
end
