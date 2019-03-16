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

class TestNullArray < Test::Unit::TestCase
  include Helper::Buildable

  def test_new
    assert_equal(build_null_array([nil, nil, nil]),
                 Arrow::NullArray.new(3))
  end

  def test_length
    builder = Arrow::NullArrayBuilder.new
    builder.append_null
    builder.append_null
    builder.append_null
    array = builder.finish
    assert_equal(3, array.length)
  end

  def test_n_nulls
    builder = Arrow::NullArrayBuilder.new
    builder.append_null
    builder.append_null
    builder.append_null
    array = builder.finish
    assert_equal(3, array.n_nulls)
  end

  def test_slice
    builder = Arrow::NullArrayBuilder.new
    builder.append_null
    builder.append_null
    builder.append_null
    array = builder.finish
    assert_equal(2, array.slice(1, 2).length)
  end
end
