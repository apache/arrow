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

class TestGandivaBinaryLiteralNode < Test::Unit::TestCase
  def setup
    omit("Gandiva is required") unless defined?(::Gandiva)
  end

  def test_value
    value = "\x00\x01\x02\x03\x04"
    literal_node = Gandiva::BinaryLiteralNode.new(value)
    assert_equal(value, literal_node.value.to_s)
  end

  def test_value_raw
    value = [0, 1, 2, 3, 4]
    literal_node = Gandiva::BinaryLiteralNode.new(value.pack("C*"))
    assert_equal(value, literal_node.value_raw)
  end
end
