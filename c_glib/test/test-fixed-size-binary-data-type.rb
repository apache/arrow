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

class TestFixedSizeBinaryDataType < Test::Unit::TestCase
  def setup
    @byte_width = 10
    @data_type = Arrow::FixedSizeBinaryDataType.new(@byte_width)
  end

  def test_type
    assert_equal(Arrow::Type::FIXED_SIZE_BINARY, @data_type.id)
  end

  def test_to_s
    assert_equal("fixed_size_binary[10]", @data_type.to_s)
  end

  def test_byte_width
    assert_equal(@byte_width, @data_type.byte_width)
  end

  def test_bit_width
    assert_equal(@byte_width * 8, @data_type.bit_width)
  end
end
