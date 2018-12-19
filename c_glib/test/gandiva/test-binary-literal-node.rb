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
    @value = "\x00\x01\x02\x03\x04"
  end

  sub_test_case(".new") do
    def test_string
      node = Gandiva::BinaryLiteralNode.new(@value)
      assert_equal(@value, node.value.to_s)
    end

    def test_bytes
      bytes_value = GLib::Bytes.new(@value)
      node = Gandiva::BinaryLiteralNode.new(bytes_value)
      assert_equal(@value, node.value.to_s)
    end
  end

  sub_test_case("instance methods") do
    def setup
      super
      @node = Gandiva::BinaryLiteralNode.new(@value)
    end

    def test_return_type
      assert_equal(Arrow::BinaryDataType.new, @node.return_type)
    end
  end
end
