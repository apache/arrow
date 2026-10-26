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

class TestDenseBitmapBuilder < Test::Unit::TestCase
  def setup
    @builder = ArrowFormat::DenseBitmapBuilder.new
  end

  def test_empty
    assert_equal(IO::Buffer.for(""), @builder.finish)
  end

  def test_1byte
    8.times do |i|
      @builder.append(i.odd?)
    end
    buffer = [0b10101010].pack("C") + "\x00" * 63
    assert_equal(IO::Buffer.for(buffer),
                 @builder.finish)
  end

  def test_9bits
    8.times do |i|
      @builder.append(i.odd?)
    end
    @builder.append(true)
    buffer = [0b10101010].pack("C") + [0b00000001].pack("C") + "\x00" * 62
    assert_equal(IO::Buffer.for(buffer),
                 @builder.finish)
  end
end
