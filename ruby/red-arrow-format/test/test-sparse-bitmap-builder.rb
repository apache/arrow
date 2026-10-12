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

class TestSparseBitmapBuilder < Test::Unit::TestCase
  def setup
    @builder = ArrowFormat::SparseBitmapBuilder.new
  end

  def test_empty
    buffer = [0b00000001].pack("C") + "\x00" * 63
    assert_equal(IO::Buffer.for(buffer),
                 @builder.finish(1))
  end

  def test_unset_0bit
    @builder.unset(0)
    buffer = [0b11111110].pack("C") + "\x00" * 63
    assert_equal(IO::Buffer.for(buffer),
                 @builder.finish(8))
  end

  def test_unset_multiple_bytes
    @builder.unset(15)
    @builder.unset(31)
    buffer =  [0b11111111].pack("C")
    buffer += [0b01111111].pack("C")
    buffer += [0b11111111].pack("C")
    buffer += [0b01111111].pack("C")
    buffer += [0b11111111].pack("C")
    buffer += "\x00" * 59
    assert_equal(IO::Buffer.for(buffer),
                 @builder.finish(40))
  end

  def test_unset_duplicated
    @builder.unset(15)
    @builder.unset(15)
    buffer =  [0b11111111].pack("C")
    buffer += [0b01111111].pack("C")
    buffer += "\x00" * 62
    assert_equal(IO::Buffer.for(buffer),
                 @builder.finish(16))
  end
end
