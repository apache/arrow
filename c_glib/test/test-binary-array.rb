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

class TestBinaryArray < Test::Unit::TestCase
  def test_value
    data = "\x00\x01\x02"
    builder = Arrow::BinaryArrayBuilder.new
    builder.append(data)
    array = builder.finish
    assert_equal(data, array.get_value(0).to_s)
  end

  def test_buffer
    data1 = "\x00\x01\x02"
    data2 = "\x03\x04\x05"
    builder = Arrow::BinaryArrayBuilder.new
    builder.append(data1)
    builder.append(data2)
    array = builder.finish
    assert_equal(data1 + data2, array.buffer.data.to_s)
  end
end
