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

class TestTake < Test::Unit::TestCase
  include Helper::Buildable
  include Helper::Omittable

  def test_no_null
    require_gi(1, 42, 0)
    indices = build_int16_array([1, 0, 2])
    assert_equal(build_int16_array([0, 1, 2]),
                 build_int16_array([1, 0 ,2]).take(indices))
  end

  def test_null
    require_gi(1, 42, 0)
    indices = build_int16_array([2, nil, 0])
    assert_equal(build_int16_array([2, nil, 1]),
                 build_int16_array([1, 0, 2]).take(indices))
  end

  def test_out_of_index
    require_gi(1, 42, 0)
    indices = build_int16_array([1, 2, 3])
    assert_raise(Arrow::Error::Invalid) do
      build_int16_array([0, 1, 2]).take(indices)
    end
  end
end
