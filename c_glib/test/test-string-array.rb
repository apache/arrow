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

class TestStringArray < Test::Unit::TestCase
  include Helper::Buildable

  def test_new
    value_offsets = Arrow::Buffer.new([0, 5, 11, 11].pack("l*"))
    data = Arrow::Buffer.new("HelloWorld!")
    assert_equal(build_string_array(["Hello", "World!", nil]),
                 Arrow::StringArray.new(3,
                                        value_offsets,
                                        data,
                                        Arrow::Buffer.new([0b011].pack("C*")),
                                        -1))
  end

  def test_value
    builder = Arrow::StringArrayBuilder.new
    builder.append("Hello")
    array = builder.finish
    assert_equal("Hello", array.get_string(0))
  end

  def test_buffer
    builder = Arrow::StringArrayBuilder.new
    builder.append("Hello")
    builder.append("World")
    array = builder.finish
    assert_equal("HelloWorld", array.buffer.data.to_s)
  end
end
