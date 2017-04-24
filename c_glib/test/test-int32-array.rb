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

class TestInt32Array < Test::Unit::TestCase
  def test_buffer
    builder = Arrow::Int32ArrayBuilder.new
    builder.append(-1)
    builder.append(2)
    builder.append(-4)
    array = builder.finish
    assert_equal([-1, 2, -4].pack("l*"), array.buffer.data.to_s)
  end

  def test_value
    builder = Arrow::Int32ArrayBuilder.new
    builder.append(-1)
    array = builder.finish
    assert_equal(-1, array.get_value(0))
  end
end
