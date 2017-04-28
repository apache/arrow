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

class TestPoolBuffer < Test::Unit::TestCase
  def setup
    @buffer = Arrow::PoolBuffer.new
  end

  def test_resize
    @buffer.resize(1)
    assert_equal(1, @buffer.size)
  end

  def test_reserve
    @buffer.reserve(1)
    assert_equal(64, @buffer.capacity)
  end
end
