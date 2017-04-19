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

class TestUInt8Tensor < Test::Unit::TestCase
  include Helper::Omittable

  def setup
    @raw_data = [
      1, 2,
      3, 4,

      5, 6,
      7, 8,

      9, 10,
      11, 12,
    ]
    data = Arrow::Buffer.new(@raw_data.pack("c*"))
    shape = [3, 2, 2]
    strides = []
    names = []
    @tensor = Arrow::UInt8Tensor.new(data, shape, strides, names)
  end

  def test_raw_data
    require_gi(3, 1, 2)
    assert_equal(@raw_data, @tensor.raw_data)
  end
end
