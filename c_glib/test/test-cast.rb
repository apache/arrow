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

class TestCast < Test::Unit::TestCase
  include Helper::Buildable
  include Helper::Omittable

  def test_safe
    require_gi(1, 42, 0)
    data = [-1, 2, nil]
    assert_equal(build_int32_array(data),
                 build_int8_array(data).cast(Arrow::Int32DataType.new))
  end

  sub_test_case("allow-int-overflow") do
    def test_default
      require_gi(1, 42, 0)
      assert_raise(Arrow::Error::Invalid) do
        build_int32_array([128]).cast(Arrow::Int8DataType.new)
      end
    end

    def test_true
      options = Arrow::CastOptions.new
      options.allow_int_overflow = true
      assert_equal(build_int8_array([-128]),
                   build_int32_array([128]).cast(Arrow::Int8DataType.new,
                                                 options))
    end
  end
end
